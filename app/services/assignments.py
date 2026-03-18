from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple

from app.core.config import settings
from app.data.repositories.base import BaseRepository
from app.models.schemas import AssignmentItem, AssignmentResponse, Task, TaskFilters, UnassignedItem
from app.services.duration_forecast import DurationForecaster
from app.services.fleet_state import FleetStateService
from app.services.routing import RoutingService
from app.services.scoring import score_task, shift_window


class AssignmentService:
    def __init__(self, repo: BaseRepository, routing: RoutingService) -> None:
        self.repo = repo
        self.routing = routing
        self.fleet_state = FleetStateService(repo, routing)
        self.duration_forecaster = DurationForecaster(repo)

    def plan(self, task_ids: List[str] | None, filters: TaskFilters | None, max_total_time_minutes: int, max_detour_ratio: float) -> AssignmentResponse:
        tasks = self._load_tasks(task_ids, filters)
        if not tasks:
            return AssignmentResponse(assignments=[], unassigned=[], summary="no tasks")
        self.duration_forecaster.fill_missing(tasks)

        unassigned: List[UnassignedItem] = []
        if settings.use_task_assignments:
            assigned_ids = {a.task_id for a in self.repo.assignments()}
            if assigned_ids:
                remaining = []
                for task in tasks:
                    if task.task_id in assigned_ids:
                        unassigned.append(UnassignedItem(task_id=task.task_id, reason="already assigned"))
                    else:
                        remaining.append(task)
                tasks = remaining
                if not tasks:
                    return AssignmentResponse(assignments=[], unassigned=unassigned, summary="no assignments")

        wells_map = self._prefetch_wells(tasks)
        groups = self._cluster_tasks(tasks, wells_map, max_total_time_minutes, max_detour_ratio)
        unit_state = self.fleet_state.build_state()

        assignments: List[AssignmentItem] = []

        for group in groups:
            result = self._choose_unit_for_group(group, wells_map, unit_state)
            if result is None:
                for t in group:
                    unassigned.append(UnassignedItem(task_id=t.task_id, reason="no feasible unit"))
                continue

            unit_id, plan_items, reason = result
            if plan_items is None:
                for t in group:
                    unassigned.append(UnassignedItem(task_id=t.task_id, reason=reason))
                continue

            assignments.extend(plan_items)
            # Update unit state to last stop (open-end)
            last_item = plan_items[-1]
            unit = unit_state[unit_id]
            unit.available_at = last_item.end_time
            unit.node_id = last_item.route_nodes[-1]
            unit.lon, unit.lat = last_item.route_coords[-1]

        summary = "assigned" if assignments else "no assignments"
        return AssignmentResponse(assignments=assignments, unassigned=unassigned, summary=summary)

    def _load_tasks(self, task_ids: List[str] | None, filters: TaskFilters | None) -> List[Task]:
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

    def _prefetch_wells(self, tasks: List[Task]):
        uwis = sorted({t.destination_uwi for t in tasks})
        wells = self.repo.wells_by_uwi(uwis)
        wells_map = {w.uwi: w for w in wells}
        missing = [uwi for uwi in uwis if uwi not in wells_map]
        if missing:
            raise RuntimeError(f"wells not found or have NULL coords: {missing}")
        return wells_map

    def _cluster_tasks(self, tasks, wells_map, max_total_time_minutes, max_detour_ratio):
        remaining = sorted(tasks, key=lambda t: (t.planned_start, t.priority))
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
        baseline_distance = self._group_baseline_distance(new_group, wells_map)
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

    def _group_route_distance(self, group, wells_map):
        ordered = self._nearest_neighbor_order(group, wells_map)
        distance = 0.0
        for a, b in zip(ordered, ordered[1:]):
            well_a = wells_map[a.destination_uwi]
            well_b = wells_map[b.destination_uwi]
            route = self.routing.route_between_points(well_a.lon, well_a.lat, well_b.lon, well_b.lat)
            distance += route["distance_km"]
        return max(distance, 0.1)

    def _group_baseline_distance(self, group, wells_map):
        total_distance = 0.0
        units = self.repo.units_snapshot()
        for task in group:
            well = wells_map[task.destination_uwi]
            best_dist = None
            for unit in units:
                route = self.routing.route_between_points(unit.pos_x, unit.pos_y, well.lon, well.lat)
                d = route["distance_km"]
                if best_dist is None or d < best_dist:
                    best_dist = d
            if best_dist is None:
                continue
            total_distance += best_dist
        return max(total_distance, 0.1)

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
                d = abs(last_well.lon - well.lon) + abs(last_well.lat - well.lat)
                if best_dist is None or d < best_dist:
                    best_dist = d
                    best_idx = idx
            ordered.append(remaining.pop(best_idx))
        return ordered

    def _choose_unit_for_group(self, group, wells_map, unit_state: Dict[int, object]) -> Tuple[int, List[AssignmentItem], str] | None:
        ordered = self._nearest_neighbor_order(group, wells_map)
        best_cost = None
        best_unit_id = None
        best_plan = None
        fail_reasons: Set[str] = set()

        for unit_id, unit in unit_state.items():
            if not self._is_unit_compatible(unit, ordered):
                fail_reasons.add("no_compatible_unit")
                continue

            plan_items = []
            current_node = unit.node_id
            current_time = unit.available_at
            total_cost = 0.0
            failed = False
            fail_reason = ""

            for task in ordered:
                well = wells_map[task.destination_uwi]
                task_node = self.routing.node_index.nearest(well.lon, well.lat)
                route = self.routing.route_between_nodes(current_node, task_node, speed_kmph=unit.speed_kmph)
                distance_km = route["distance_km"]
                travel_minutes = route["time_minutes"]

                score_result = score_task(
                    task=task,
                    distance_km=distance_km,
                    travel_minutes=travel_minutes,
                    unit_available_at=current_time,
                )

                _, shift_end = shift_window(task.planned_start)
                if score_result.start_time > shift_end:
                    failed = True
                    fail_reason = f"shift_window_miss:{task.task_id}"
                    break

                total_cost += score_result.cost
                plan_items.append(
                    AssignmentItem(
                        task_id=task.task_id,
                        wialon_id=unit.wialon_id,
                        eta_minutes=score_result.eta_minutes,
                        distance_km=distance_km,
                        score=score_result.score,
                        reason=score_result.reason,
                        start_time=score_result.start_time,
                        end_time=score_result.end_time,
                        route_nodes=route["nodes"],
                        route_coords=route["coords"],
                    )
                )

                current_time = score_result.end_time
                current_node = task_node

            if failed:
                fail_reasons.add(fail_reason)
                continue
            if best_cost is None or total_cost < best_cost:
                best_cost = total_cost
                best_unit_id = unit_id
                best_plan = plan_items

        if best_unit_id is None or best_plan is None:
            reason = "; ".join(sorted(fail_reasons)) if fail_reasons else "no feasible unit"
            return None, None, reason
        return best_unit_id, best_plan, ""

    def _is_unit_compatible(self, unit, tasks: List[Task]) -> bool:
        rules = self.repo.compatibility()
        if not rules:
            return True
        compat: Dict[str, Set[str]] = {}
        for rule in rules:
            compat.setdefault(rule.task_type, set()).add(rule.unit_type)
        for task in tasks:
            if task.task_type == "unknown":
                continue
            if task.task_type not in compat:
                return False
            if unit.unit_type not in compat[task.task_type]:
                return False
        return True
