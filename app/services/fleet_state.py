from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import median
from typing import Dict, Iterable, List

from app.core.config import settings
from app.data.repositories.base import BaseRepository
from app.models.schemas import Task, WialonUnitSnapshot
from app.services.routing import RoutingService
from app.utils.geo import distance_km


@dataclass
class UnitState:
    wialon_id: int
    name: str
    unit_type: str
    node_id: int
    lon: float
    lat: float
    available_at: datetime
    speed_kmph: float


class FleetStateService:
    def __init__(self, repo: BaseRepository, routing: RoutingService) -> None:
        self.repo = repo
        self.routing = routing

    def build_state(
        self,
        tasks: List[Task] | None = None,
        anchor_time: datetime | None = None,
    ) -> Dict[int, UnitState]:
        snapshots = self.repo.units_snapshots_history()
        if not snapshots:
            raise RuntimeError("units_snapshot history is empty")
        anchor = None
        if settings.use_snapshot_by_planning_date:
            anchor = self._normalize_anchor_time(anchor_time, tasks)
            latest = self._snapshots_by_anchor(snapshots, anchor)
        else:
            latest = self._latest_snapshots(snapshots)
        speeds = self._avg_speeds(snapshots)

        state: Dict[int, UnitState] = {}
        for unit in latest:
            node_id = self.routing.node_index.nearest(unit.pos_x, unit.pos_y)
            speed = speeds.get(unit.wialon_id, settings.avg_speed_kmph)
            available_at = unit.pos_t
            if anchor is not None and settings.anchor_units_at_plan_start:
                available_at = anchor

            state[unit.wialon_id] = UnitState(
                wialon_id=unit.wialon_id,
                name=unit.name,
                unit_type=unit.unit_type,
                node_id=node_id,
                lon=unit.pos_x,
                lat=unit.pos_y,
                available_at=available_at,
                speed_kmph=speed,
            )

        return state

    def _latest_snapshots(self, snapshots: List[WialonUnitSnapshot]) -> List[WialonUnitSnapshot]:
        latest: Dict[int, WialonUnitSnapshot] = {}
        for snap in snapshots:
            existing = latest.get(snap.wialon_id)
            if existing is None or snap.pos_t > existing.pos_t:
                latest[snap.wialon_id] = snap
        return list(latest.values())

    def _snapshots_by_anchor(
        self, snapshots: List[WialonUnitSnapshot], anchor_time: datetime | None
    ) -> List[WialonUnitSnapshot]:
        if anchor_time is None:
            return self._latest_snapshots(snapshots)

        best_before: Dict[int, WialonUnitSnapshot] = {}
        best_after: Dict[int, WialonUnitSnapshot] = {}
        for snap in snapshots:
            if snap.pos_t <= anchor_time:
                existing = best_before.get(snap.wialon_id)
                if existing is None or snap.pos_t > existing.pos_t:
                    best_before[snap.wialon_id] = snap
            else:
                existing = best_after.get(snap.wialon_id)
                if existing is None or snap.pos_t < existing.pos_t:
                    best_after[snap.wialon_id] = snap

        result: List[WialonUnitSnapshot] = []
        for unit_id in set(best_before) | set(best_after):
            if unit_id in best_before:
                result.append(best_before[unit_id])
            else:
                result.append(best_after[unit_id])
        return result

    def _normalize_anchor_time(self, anchor_time: datetime | None, tasks: List[Task] | None) -> datetime | None:
        if anchor_time is None and tasks:
            anchor_time = min(t.planned_start for t in tasks)
        if anchor_time is None:
            return None
        if anchor_time.tzinfo is not None:
            return anchor_time.astimezone(timezone.utc).replace(tzinfo=None)
        return anchor_time

    def _avg_speeds(self, snapshots: List[WialonUnitSnapshot]) -> Dict[int, float]:
        by_unit: Dict[int, List[WialonUnitSnapshot]] = {}
        for snap in snapshots:
            by_unit.setdefault(snap.wialon_id, []).append(snap)

        speeds: Dict[int, float] = {}
        for unit_id, items in by_unit.items():
            if len(items) < 2:
                continue
            items_sorted = sorted(items, key=lambda s: s.pos_t)
            segment_speeds: List[float] = []
            for prev, curr in zip(items_sorted, items_sorted[1:]):
                dt = (curr.pos_t - prev.pos_t).total_seconds() / 3600.0
                if dt <= 0:
                    continue
                d = distance_km(prev.pos_x, prev.pos_y, curr.pos_x, curr.pos_y)
                if d <= 0:
                    continue
                speed = d / dt
                if speed < settings.min_speed_kmph or speed > settings.max_speed_kmph:
                    continue
                segment_speeds.append(speed)
            if segment_speeds:
                speeds[unit_id] = median(segment_speeds)
        return speeds
