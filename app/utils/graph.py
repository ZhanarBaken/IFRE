from __future__ import annotations

import heapq
from typing import Dict, List, Tuple

from app.models.schemas import RoadEdge, RoadNode
from app.utils.geo import distance_km


class _KDNode:
    __slots__ = ("node_id", "lon", "lat", "left", "right", "axis")

    def __init__(self, node_id: int, lon: float, lat: float, axis: int):
        self.node_id = node_id
        self.lon = lon
        self.lat = lat
        self.left = None
        self.right = None
        self.axis = axis


class NodeIndex:
    def __init__(self, nodes: List[RoadNode]) -> None:
        self.nodes = nodes
        self.coords = {node.node_id: (node.lon, node.lat) for node in nodes}
        self.root = self._build(nodes, depth=0)

    def _build(self, nodes: List[RoadNode], depth: int):
        if not nodes:
            return None
        axis = depth % 2
        nodes_sorted = sorted(nodes, key=lambda n: (n.lon, n.lat) if axis == 0 else (n.lat, n.lon))
        median = len(nodes_sorted) // 2
        node = nodes_sorted[median]
        root = _KDNode(node.node_id, node.lon, node.lat, axis)
        root.left = self._build(nodes_sorted[:median], depth + 1)
        root.right = self._build(nodes_sorted[median + 1 :], depth + 1)
        return root

    def nearest(self, lon: float, lat: float) -> int:
        if self.root is None:
            raise ValueError("No nodes loaded")

        best = {"id": None, "dist": float("inf")}

        def _search(node: _KDNode | None):
            if node is None:
                return
            d = distance_km(lon, lat, node.lon, node.lat)
            if d < best["dist"]:
                best["dist"] = d
                best["id"] = node.node_id
            axis = node.axis
            diff = (lon - node.lon) if axis == 0 else (lat - node.lat)
            first, second = (node.left, node.right) if diff < 0 else (node.right, node.left)
            _search(first)
            if abs(diff) * 111.0 < best["dist"]:
                _search(second)

        _search(self.root)
        if best["id"] is None:
            raise ValueError("No nodes loaded")
        return int(best["id"])


class Graph:
    def __init__(self, nodes: List[RoadNode], edges: List[RoadEdge]) -> None:
        self.nodes = nodes
        self.coords: Dict[int, Tuple[float, float]] = {
            node.node_id: (node.lon, node.lat) for node in nodes
        }
        self.adj: Dict[int, List[Tuple[int, float]]] = {node.node_id: [] for node in nodes}
        for edge in edges:
            self.adj.setdefault(edge.source, []).append((edge.target, edge.weight))
        self._cache: Dict[Tuple[int, int], Tuple[float, List[int]]] = {}

    def shortest_path(self, start: int, end: int) -> Tuple[float, List[int]]:
        cache_key = (start, end)
        if cache_key in self._cache:
            return self._cache[cache_key]
        if start == end:
            result = (0.0, [start])
            self._cache[cache_key] = result
            return result

        dist: Dict[int, float] = {start: 0.0}
        prev: Dict[int, int] = {}
        heap = [(0.0, start)]

        while heap:
            current_dist, current = heapq.heappop(heap)
            if current == end:
                break
            if current_dist > dist.get(current, float("inf")):
                continue
            for neighbor, weight in self.adj.get(current, []):
                cand = current_dist + weight
                if cand < dist.get(neighbor, float("inf")):
                    dist[neighbor] = cand
                    prev[neighbor] = current
                    heapq.heappush(heap, (cand, neighbor))

        if end not in dist:
            raise ValueError("No path found")

        path = [end]
        while path[-1] != start:
            path.append(prev[path[-1]])
        path.reverse()
        result = (dist[end], path)
        self._cache[cache_key] = result
        return result

    def shortest_paths_from(self, start: int, targets: List[int] | None = None) -> Dict[int, float]:
        dist: Dict[int, float] = {start: 0.0}
        heap = [(0.0, start)]
        remaining = set(targets) if targets else None

        while heap:
            current_dist, current = heapq.heappop(heap)
            if current_dist > dist.get(current, float("inf")):
                continue
            if remaining and current in remaining:
                remaining.remove(current)
                if not remaining:
                    break
            for neighbor, weight in self.adj.get(current, []):
                cand = current_dist + weight
                if cand < dist.get(neighbor, float("inf")):
                    dist[neighbor] = cand
                    heapq.heappush(heap, (cand, neighbor))
        return dist

    def path_coords(self, path: List[int]) -> List[List[float]]:
        coords = []
        for node_id in path:
            lon, lat = self.coords[node_id]
            coords.append([lon, lat])
        return coords
