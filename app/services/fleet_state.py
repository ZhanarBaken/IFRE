from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List

from app.core.config import settings
from app.data.repositories.base import BaseRepository
from app.models.schemas import Task, TaskAssignment, WialonUnitSnapshot
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

    def build_state(self, tasks: List[Task] | None = None, assignments: List[TaskAssignment] | None = None) -> Dict[int, UnitState]:
        snapshots = self.repo.units_snapshots_history()
        if not snapshots:
            raise RuntimeError("units_snapshot history is empty")
        latest = self._latest_snapshots(snapshots)
        speeds = self._avg_speeds(snapshots)

        state: Dict[int, UnitState] = {}
        for unit in latest:
            node_id = self.routing.node_index.nearest(unit.pos_x, unit.pos_y)
            speed = speeds.get(unit.wialon_id, settings.avg_speed_kmph)
            state[unit.wialon_id] = UnitState(
                wialon_id=unit.wialon_id,
                name=unit.name,
                unit_type=unit.unit_type,
                node_id=node_id,
                lon=unit.pos_x,
                lat=unit.pos_y,
                available_at=unit.pos_t,
                speed_kmph=speed,
            )

        if settings.use_task_assignments:
            if assignments is None:
                assignments = self.repo.assignments()
            if assignments:
                task_ids = [a.task_id for a in assignments]
                tasks_map = {t.task_id: t for t in self.repo.tasks_by_ids(task_ids)}
                active = []
                for assign in assignments:
                    status = (assign.status or "").lower()
                    if status in {"done", "completed", "closed"}:
                        continue
                    task = tasks_map.get(assign.task_id)
                    if not task:
                        continue
                    active.append((task, assign))
                if active:
                    self._apply_assignments_to_state(state, active)
        return state

    def _latest_snapshots(self, snapshots: List[WialonUnitSnapshot]) -> List[WialonUnitSnapshot]:
        latest: Dict[int, WialonUnitSnapshot] = {}
        for snap in snapshots:
            existing = latest.get(snap.wialon_id)
            if existing is None or snap.pos_t > existing.pos_t:
                latest[snap.wialon_id] = snap
        return list(latest.values())

    def _avg_speeds(self, snapshots: List[WialonUnitSnapshot]) -> Dict[int, float]:
        by_unit: Dict[int, List[WialonUnitSnapshot]] = {}
        for snap in snapshots:
            by_unit.setdefault(snap.wialon_id, []).append(snap)

        speeds: Dict[int, float] = {}
        for unit_id, items in by_unit.items():
            if len(items) < 2:
                continue
            items_sorted = sorted(items, key=lambda s: s.pos_t)
            total_distance = 0.0
            total_hours = 0.0
            for prev, curr in zip(items_sorted, items_sorted[1:]):
                dt = (curr.pos_t - prev.pos_t).total_seconds() / 3600.0
                if dt <= 0:
                    continue
                d = distance_km(prev.pos_x, prev.pos_y, curr.pos_x, curr.pos_y)
                if d <= 0:
                    continue
                total_distance += d
                total_hours += dt
            if total_hours > 0:
                speeds[unit_id] = total_distance / total_hours
        return speeds

    def _apply_assignments_to_state(
        self, state: Dict[int, UnitState], items: List[tuple[Task, TaskAssignment]]
    ) -> None:
        for task, assign in items:
            unit = state.get(assign.wialon_id)
            if unit is None:
                continue
            start_time = assign.actual_start or task.planned_start
            end_time = start_time + timedelta(hours=task.duration_hours)
            if end_time > unit.available_at:
                # Move unit to task destination and mark busy until end_time.
                # If we cannot resolve well coordinates, keep current location.
                try:
                    well = self.repo.well_by_uwi(task.destination_uwi)
                    if well:
                        unit.node_id = self.routing.node_index.nearest(well.lon, well.lat)
                        unit.lon = well.lon
                        unit.lat = well.lat
                except Exception:
                    pass
                unit.available_at = end_time
