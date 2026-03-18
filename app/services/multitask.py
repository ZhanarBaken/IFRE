from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Set

from app.core.config import settings
from app.data.repositories.base import BaseRepository
from app.models.schemas import Task, TaskFilters
from app.services.duration_forecast import DurationForecaster
from app.services.routing import RoutingService
from app.services.scoring import shift_window
from app.utils.geo import distance_km


class MultitaskService:
    def __init__(self, repo: BaseRepository, routing: RoutingService) -> None:
        self.repo = repo
        self.routing = routing
        self.duration_forecaster = DurationForecaster(repo)

    def evaluate(self, task_ids: List[str] | None, filters: TaskFilters | None, max_total_time_minutes: int, max_detour_ratio: float):
        tasks = self._load_tasks(task_ids, filters)
        if not tasks:
            return self._empty()
        self.duration_forecaster.fill_missing(tasks)

        wells_map = self._prefetch_wells(tasks)
        groups = self._cluster_tasks(tasks, wells_map, max_total_time_minutes, max_detour_ratio)
        baseline_distance_km, baseline_time_minutes = self._baseline(tasks, wells_map)
        total_distance_km, total_time_minutes = self._estimate_group_metrics(groups, wells_map)

        if total_distance_km <= 0:
            return self._empty()

        savings_percent = max(0.0, (1.0 - total_distance_km / baseline_distance_km) * 100.0)
        strategy = "separate" if len(groups) == len(tasks) else "mixed"
        if len(groups) == 1 and len(tasks) > 1:
            strategy = "single_unit"

        reason = "grouping based on proximity, detour, shift and compatibility constraints"
        return {
            "groups": [[t.task_id for t in group] for group in groups],
            "strategy_summary": strategy,
            "total_distance_km": round(total_distance_km, 2),
            "total_time_minutes": int(round(total_time_minutes)),
            "baseline_distance_km": round(baseline_distance_km, 2),
            "baseline_time_minutes": int(round(baseline_time_minutes)),
            "savings_percent": round(savings_percent, 1),
            "reason": reason,
        }

    def _load_tasks(self, task_ids: List[str] | None, filters: TaskFilters | None):
        if task_ids:
            tasks = self.repo.tasks_by_ids(task_ids)
            found = {t.task_id for t in tasks}
            missing = [tid for tid in task_ids if tid not in found]
            if missing:
                raise RuntimeError(f"tasks not found: {missing}")
            return tasks
        if filters:
            start_dt, end_dt = self._resolve_window(filters)
            return self.repo.tasks_by_window(start_dt, end_dt, limit=filters.limit)
        raise RuntimeError("task_ids or filters are required")

    def _resolve_window(self, filters: TaskFilters):
        if filters.shift:
            if not filters.start_date:
                raise RuntimeError("filters.start_date is required when shift is set")
            if filters.end_date and filters.end_date != filters.start_date:
                raise RuntimeError("shift filter supports only a single date")
            if filters.shift == "day":
                start_dt = datetime.combine(filters.start_date, datetime.min.time()) + timedelta(hours=8)
                end_dt = datetime.combine(filters.start_date, datetime.min.time()) + timedelta(hours=20)
            elif filters.shift == "night":
                start_dt = datetime.combine(filters.start_date, datetime.min.time()) + timedelta(hours=20)
                end_dt = datetime.combine(filters.start_date + timedelta(days=1), datetime.min.time()) + timedelta(hours=8)
            else:
                raise RuntimeError("filters.shift must be 'day' or 'night'")
            return start_dt, end_dt

        if not filters.start_date:
            raise RuntimeError("filters.start_date is required")
        end_date = filters.end_date or filters.start_date
        start_dt = datetime.combine(filters.start_date, datetime.min.time())
        end_dt = datetime.combine(end_date + timedelta(days=1), datetime.min.time())
        return start_dt, end_dt

    def _prefetch_wells(self, tasks) -> Dict[str, object]:
        uwis = sorted({t.destination_uwi for t in tasks})
        wells = self.repo.wells_by_uwi(uwis)
        wells_map = {w.uwi: w for w in wells}
        missing = [uwi for uwi in uwis if uwi not in wells_map]
        if missing:
            raise RuntimeError(f"wells not found or have NULL coords: {missing}")
        return wells_map

    def _baseline(self, tasks, wells_map):
        total_distance = 0.0
        total_time = 0.0
        units = self.repo.units_snapshot()
        if not units:
            raise RuntimeError("units_snapshot is empty")
        for task in tasks:
            well = wells_map[task.destination_uwi]
            best_dist = None
            for unit in units:
                route = self.routing.route_between_points(unit.pos_x, unit.pos_y, well.lon, well.lat)
                d = route["distance_km"]
                if best_dist is None or d < best_dist:
                    best_dist = d
            if best_dist is None:
                raise RuntimeError("no units available for baseline")
            total_distance += best_dist
            total_time += best_dist / settings.avg_speed_kmph * 60.0 + task.duration_hours * 60.0
        return max(total_distance, 0.01), max(total_time, 1.0)

    def _cluster_tasks(self, tasks, wells_map, max_total_time_minutes, max_detour_ratio):
        remaining = tasks[:]
        groups = []
        while remaining:
            base = remaining.pop(0)
            group = [base]
            changed = True
            while changed:
                changed = False
                for other in list(remaining):
                    if self._can_add(group, other, wells_map, max_total_time_minutes, max_detour_ratio):
                        group.append(other)
                        remaining.remove(other)
                        changed = True
            groups.append(group)
        return groups

    def _can_add(self, group, candidate, wells_map, max_total_time_minutes, max_detour_ratio):
        new_group = group + [candidate]
        if not self._has_compatible_unit(new_group):
            return False
        if not self._sla_feasible(new_group, wells_map):
            return False
        route_distance = self._group_route_distance(new_group, wells_map)
        baseline_distance, _baseline_time = self._group_baseline(new_group, wells_map)
        detour_ratio = route_distance / baseline_distance if baseline_distance > 0 else 999.0
        total_time = route_distance / settings.avg_speed_kmph * 60.0 + sum(
            t.duration_hours * 60.0 for t in new_group
        )
        return detour_ratio <= max_detour_ratio and total_time <= max_total_time_minutes

    def _has_compatible_unit(self, tasks: List[Task]) -> bool:
        rules = self.repo.compatibility()
        if not rules:
            return True
        compat: Dict[str, Set[str]] = {}
        for rule in rules:
            compat.setdefault(rule.task_type, set()).add(rule.unit_type)
        units = self.repo.units_snapshot()
        if not units:
            return False
        for unit in units:
            ok = True
            for task in tasks:
                if task.task_type == "unknown":
                    continue
                if task.task_type not in compat:
                    ok = False
                    break
                if unit.unit_type not in compat[task.task_type]:
                    ok = False
                    break
            if ok:
                return True
        return False

    def _sla_feasible(self, tasks: List[Task], wells_map) -> bool:
        ordered = self._nearest_neighbor_order(tasks, wells_map)
        current_time = ordered[0].planned_start
        for idx, task in enumerate(ordered):
            if idx == 0:
                arrival_time = current_time
            else:
                prev = ordered[idx - 1]
                well_a = wells_map[prev.destination_uwi]
                well_b = wells_map[task.destination_uwi]
                route = self.routing.route_between_points(well_a.lon, well_a.lat, well_b.lon, well_b.lat)
                travel_minutes = route["time_minutes"]
                arrival_time = current_time + timedelta(minutes=travel_minutes)

            start_time = max(arrival_time, task.planned_start)
            _, shift_end = shift_window(task.planned_start)
            if start_time > shift_end:
                return False
            current_time = start_time + timedelta(hours=task.duration_hours)
        return True

    def _group_baseline(self, group, wells_map):
        total_distance, total_time = self._baseline(group, wells_map)
        return total_distance, total_time

    def _group_route_distance(self, group, wells_map):
        if len(group) == 1:
            return 0.1
        ordered = self._nearest_neighbor_order(group, wells_map)
        distance = 0.0
        for a, b in zip(ordered, ordered[1:]):
            well_a = wells_map[a.destination_uwi]
            well_b = wells_map[b.destination_uwi]
            route = self.routing.route_between_points(well_a.lon, well_a.lat, well_b.lon, well_b.lat)
            distance += route["distance_km"]
        return max(distance, 0.1)

    def _nearest_neighbor_order(self, group, wells_map):
        remaining = group[:]
        ordered = [remaining.pop(0)]
        while remaining:
            last = ordered[-1]
            last_well = wells_map[last.destination_uwi]
            best_idx = 0
            best_dist = None
            for idx, candidate in enumerate(remaining):
                well = wells_map[candidate.destination_uwi]
                d = distance_km(last_well.lon, last_well.lat, well.lon, well.lat)
                if best_dist is None or d < best_dist:
                    best_dist = d
                    best_idx = idx
            ordered.append(remaining.pop(best_idx))
        return ordered

    def _estimate_group_metrics(self, groups, wells_map):
        total_distance = 0.0
        total_time = 0.0
        for group in groups:
            distance = self._group_route_distance(group, wells_map)
            total_distance += distance
            total_time += distance / settings.avg_speed_kmph * 60.0 + sum(
                t.duration_hours * 60.0 for t in group
            )
        return total_distance, total_time

    def _empty(self):
        return {
            "groups": [],
            "strategy_summary": "separate",
            "total_distance_km": 0.0,
            "total_time_minutes": 0,
            "baseline_distance_km": 0.0,
            "baseline_time_minutes": 0,
            "savings_percent": 0.0,
            "reason": "no tasks",
        }
