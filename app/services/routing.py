from __future__ import annotations

from app.core.config import settings
from app.data.repositories.base import BaseRepository
from app.utils.graph import Graph, NodeIndex


class RoutingService:
    def __init__(self, repo: BaseRepository, graph: Graph, node_index: NodeIndex) -> None:
        self.repo = repo
        self.graph = graph
        self.node_index = node_index

    def _snap(self, lon: float, lat: float) -> int:
        return self.node_index.nearest(lon, lat)

    def _distance_km(self, raw_distance: float) -> float:
        if settings.edge_weight_in_meters:
            return raw_distance / 1000.0
        return raw_distance

    def route_between_points_or_none(self, start_lon: float, start_lat: float, end_lon: float, end_lat: float):
        start_node = self._snap(start_lon, start_lat)
        end_node = self._snap(end_lon, end_lat)
        result = self.graph.shortest_path_or_none(start_node, end_node)
        if result is None:
            return None
        raw_distance, path = result
        distance_km = self._distance_km(raw_distance)
        time_minutes = int(round(distance_km / settings.avg_speed_kmph * 60.0))
        coords = self.graph.path_coords(path)
        return {
            "distance_km": round(distance_km, 2),
            "time_minutes": time_minutes,
            "nodes": path,
            "coords": coords,
            "start_node": start_node,
            "end_node": end_node,
        }

    def route_between_points(self, start_lon: float, start_lat: float, end_lon: float, end_lat: float):
        payload = self.route_between_points_or_none(start_lon, start_lat, end_lon, end_lat)
        if payload is None:
            raise ValueError("No path found")
        return payload

    def route_from_unit_to_well(self, wialon_id: int, uwi: str):
        unit = self.repo.unit_by_id(wialon_id)
        well = self.repo.well_by_uwi(uwi)
        if unit is None:
            raise ValueError(f"Unit not found: {wialon_id}")
        if well is None:
            raise ValueError(f"Well not found: {uwi}")
        return self.route_between_points(unit.pos_x, unit.pos_y, well.lon, well.lat)

    def distance_time_matrix(self, start_nodes: list[int], end_nodes: list[int]):
        matrix = {}
        for start in start_nodes:
            dist_map = self.graph.shortest_paths_from(start, targets=end_nodes)
            for end in end_nodes:
                raw_dist = dist_map.get(end)
                if raw_dist is None:
                    continue
                distance_km = self._distance_km(raw_dist)
                time_minutes = int(round(distance_km / settings.avg_speed_kmph * 60.0))
                matrix[(start, end)] = (round(distance_km, 2), time_minutes)
        return matrix

    def route_between_nodes_or_none(self, start_node: int, end_node: int, speed_kmph: float | None = None):
        result = self.graph.shortest_path_or_none(start_node, end_node)
        if result is None:
            return None
        raw_distance, path = result
        distance_km = self._distance_km(raw_distance)
        speed = speed_kmph or settings.avg_speed_kmph
        time_minutes = int(round(distance_km / speed * 60.0))
        coords = self.graph.path_coords(path)
        return {
            "distance_km": round(distance_km, 2),
            "time_minutes": time_minutes,
            "nodes": path,
            "coords": coords,
        }

    def route_between_nodes(self, start_node: int, end_node: int, speed_kmph: float | None = None):
        payload = self.route_between_nodes_or_none(start_node, end_node, speed_kmph=speed_kmph)
        if payload is None:
            raise ValueError("No path found")
        return payload
