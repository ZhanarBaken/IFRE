from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple

from app.core.config import settings
from app.data.repositories.base import BaseRepository
from app.models.schemas import AssignmentItem, AssignmentResponse, Task, TaskFilters, UnassignedItem
from app.services.compatibility import build_compat_index, compatibility_status
from app.services.duration_forecast import DurationForecaster
from app.services.fleet_state import FleetStateService
from app.services.reason_ai import ReasonAIService
from app.services.routing import RoutingService
from app.services.scoring import score_task


class AssignmentService:
    def __init__(self, repo: BaseRepository, routing: RoutingService) -> None:
        self.repo = repo
        self.routing = routing
        self.fleet_state = FleetStateService(repo, routing)
        self.duration_forecaster = DurationForecaster(repo)
        self.reason_ai = ReasonAIService()

    def plan(
        self,
        task_ids: List[str] | None,
        filters: TaskFilters | None,
        max_total_time_minutes: int,
        max_detour_ratio: float,
        grouping: bool | None = None,
    ) -> AssignmentResponse:
        tasks = self._load_tasks(task_ids, filters)
        if not tasks:
            return AssignmentResponse(assignments=[], unassigned=[], summary="no tasks")
        self.duration_forecaster.fill_missing(tasks)

        unassigned: List[UnassignedItem] = []

        wells_map = self._prefetch_wells(tasks)
        rules = self.repo.compatibility()
        has_rules = bool(rules)
        compat: Dict[str, Set[str]] = {}
        compat_norm: Dict[str, Set[str]] = {}
        if has_rules:
            compat, compat_norm = build_compat_index(rules)
        anchor_time = min(t.planned_start for t in tasks) if tasks else None
        unit_state = self.fleet_state.build_state(tasks=tasks, anchor_time=anchor_time)

        use_grouping = settings.assignments_grouping if grouping is None else grouping
        if use_grouping:
            assignments, extra_unassigned = self._plan_grouped(
                tasks,
                wells_map,
                unit_state,
                max_total_time_minutes,
                max_detour_ratio,
                has_rules,
                compat,
                compat_norm,
            )
        else:
            assignments, extra_unassigned = self._plan_time_aware(
                tasks,
                wells_map,
                unit_state,
                max_total_time_minutes,
                has_rules,
                compat,
                compat_norm,
            )
        unassigned.extend(extra_unassigned)
        self._rewrite_reasons(assignments, unassigned)

        summary = "assigned" if assignments else "no assignments"
        return AssignmentResponse(assignments=assignments, unassigned=unassigned, summary=summary)

    def _rewrite_reasons(
        self,
        assignments: List[AssignmentItem],
        unassigned: List[UnassignedItem],
    ) -> None:
        if assignments:
            assignment_reasons = self.reason_ai.rewrite_many(
                [item.reason for item in assignments],
                context="assignments_assigned",
            )
            if len(assignment_reasons) == len(assignments):
                for item, reason in zip(assignments, assignment_reasons):
                    item.reason = reason
        if unassigned:
            unassigned_reasons = self.reason_ai.rewrite_many(
                [item.reason for item in unassigned],
                context="assignments_unassigned",
            )
            if len(unassigned_reasons) == len(unassigned):
                for item, reason in zip(unassigned, unassigned_reasons):
                    item.reason = reason

    def _plan_grouped(
        self,
        tasks: List[Task],
        wells_map,
        unit_state: Dict[int, object],
        max_total_time_minutes: int,
        max_detour_ratio: float,
        has_rules: bool,
        compat: Dict[str, Set[str]],
        compat_norm: Dict[str, Set[str]],
    ) -> tuple[List[AssignmentItem], List[UnassignedItem]]:
        assignments: List[AssignmentItem] = []
        unassigned: List[UnassignedItem] = []

        task_nodes: Dict[str, int] = {}
        tasks_by_id: Dict[str, Task] = {t.task_id: t for t in tasks}
        for task in tasks:
            well = wells_map[task.destination_uwi]
            task_nodes[task.task_id] = self.routing.node_index.nearest(well.lon, well.lat)

        unit_minutes: Dict[int, float] = {uid: 0.0 for uid in unit_state.keys()}

        groups = self._cluster_tasks(
            tasks,
            wells_map,
            max_total_time_minutes,
            max_detour_ratio,
            has_rules,
            compat,
            compat_norm,
        )
        groups_sorted = sorted(groups, key=lambda g: min(t.planned_start for t in g))

        for group in groups_sorted:
            unit_id, plan_items, total_minutes, _reason = self._choose_unit_for_group(
                group,
                wells_map,
                unit_state,
                has_rules,
                compat,
                compat_norm,
                max_total_time_minutes,
                unit_minutes,
            )
            if not plan_items:
                group_reason = self._group_failure_reason(
                    group=group,
                    wells_map=wells_map,
                    task_nodes=task_nodes,
                    unit_state=unit_state,
                    has_rules=has_rules,
                    compat=compat,
                    compat_norm=compat_norm,
                    max_total_time_minutes=max_total_time_minutes,
                    unit_minutes=unit_minutes,
                )
                for task in group:
                    unassigned.append(
                        UnassignedItem(
                            task_id=task.task_id,
                            reason=group_reason,
                        )
                    )
                continue

            assignments.extend(plan_items)
            unit = unit_state.get(unit_id)
            if unit is not None:
                last_item = plan_items[-1]
                unit.available_at = last_item.end_time
                if last_item.route_nodes:
                    unit.node_id = last_item.route_nodes[-1]
                last_task = tasks_by_id[last_item.task_id]
                well = wells_map[last_task.destination_uwi]
                unit.lon = well.lon
                unit.lat = well.lat
            if unit_id in unit_minutes:
                unit_minutes[unit_id] += total_minutes

        return assignments, unassigned


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

    def _cluster_tasks(
        self,
        tasks,
        wells_map,
        max_total_time_minutes,
        max_detour_ratio,
        has_rules: bool,
        compat: Dict[str, Set[str]],
        compat_norm: Dict[str, Set[str]],
    ):
        remaining = sorted(tasks, key=lambda t: (t.planned_start, t.priority))
        groups = []
        while remaining:
            base = remaining.pop(0)
            group = [base]
            changed = True
            while changed:
                changed = False
                for other in list(remaining):
                    if self._can_add(
                        group,
                        other,
                        wells_map,
                        max_total_time_minutes,
                        max_detour_ratio,
                        has_rules,
                        compat,
                        compat_norm,
                    ):
                        group.append(other)
                        remaining.remove(other)
                        changed = True
            groups.append(group)
        return groups

    def _plan_time_aware(
        self,
        tasks: List[Task],
        wells_map,
        unit_state: Dict[int, object],
        max_total_time_minutes: int,
        has_rules: bool,
        compat: Dict[str, Set[str]],
        compat_norm: Dict[str, Set[str]],
    ) -> tuple[List[AssignmentItem], List[UnassignedItem]]:
        assignments: List[AssignmentItem] = []
        unassigned: List[UnassignedItem] = []

        remaining: Dict[str, Task] = {t.task_id: t for t in tasks}
        task_nodes: Dict[str, int] = {}
        for task in tasks:
            well = wells_map[task.destination_uwi]
            task_nodes[task.task_id] = self.routing.node_index.nearest(well.lon, well.lat)

        unit_minutes: Dict[int, float] = {uid: 0.0 for uid in unit_state.keys()}

        while remaining:
            best = None

            for task in remaining.values():
                task_node = task_nodes[task.task_id]
                well = wells_map[task.destination_uwi]
                for unit in unit_state.values():
                    status = True
                    if has_rules:
                        status = compatibility_status(task.task_type, unit.unit_type, compat, compat_norm)
                        if status is False or status is None:
                            if settings.compatibility_strict:
                                continue

                    route = self.routing.route_between_nodes_or_none(
                        unit.node_id, task_node, speed_kmph=unit.speed_kmph
                    )
                    if route is None:
                        continue

                    travel_minutes = route["time_minutes"]
                    if unit_minutes[unit.wialon_id] > 0 and max_total_time_minutes:
                        projected = unit_minutes[unit.wialon_id] + travel_minutes + task.duration_hours * 60.0
                        if projected > max_total_time_minutes:
                            continue

                    compat_penalty = 0.0
                    if has_rules and status is False and not settings.compatibility_strict:
                        compat_penalty = settings.compatibility_penalty

                    score_result = score_task(
                        task=task,
                        distance_km=route["distance_km"],
                        travel_minutes=travel_minutes,
                        unit_available_at=unit.available_at,
                        compatibility_penalty=compat_penalty,
                    )

                    if best is None or score_result.cost < best["score"].cost:
                        best = {
                            "task": task,
                            "unit": unit,
                            "route": route,
                            "score": score_result,
                            "task_node": task_node,
                            "well": well,
                        }

            if best is None:
                # No feasible assignments for remaining tasks.
                for task in remaining.values():
                    reason = self._unassigned_reason(
                        task, wells_map, task_nodes, unit_state, has_rules, compat, compat_norm, max_total_time_minutes, unit_minutes
                    )
                    unassigned.append(
                        UnassignedItem(
                            task_id=task.task_id,
                            reason=reason,
                        )
                    )
                break

            task = best["task"]
            unit = best["unit"]
            route = best["route"]
            score = best["score"]

            assignments.append(
                AssignmentItem(
                    task_id=task.task_id,
                    wialon_id=unit.wialon_id,
                    eta_minutes=score.eta_minutes,
                    distance_km=route["distance_km"],
                    score=score.score,
                    reason=score.reason,
                    planned_duration_hours=task.duration_hours,
                    planned_start=task.planned_start,
                    start_time=score.start_time,
                    end_time=score.end_time,
                    route_nodes=route["nodes"],
                    route_coords=route["coords"],
                )
            )

            unit.available_at = score.end_time
            unit.node_id = best["task_node"]
            unit.lon = best["well"].lon
            unit.lat = best["well"].lat
            unit_minutes[unit.wialon_id] += route["time_minutes"] + task.duration_hours * 60.0

            remaining.pop(task.task_id, None)

        return assignments, unassigned

    def _unassigned_reason(
        self,
        task: Task,
        wells_map,
        task_nodes: Dict[str, int],
        unit_state: Dict[int, object],
        has_rules: bool,
        compat: Dict[str, Set[str]],
        compat_norm: Dict[str, Set[str]],
        max_total_time_minutes: int,
        unit_minutes: Dict[int, float],
    ) -> str:
        total_units = len(unit_state)
        if not unit_state:
            return "Не назначена: в снапшоте нет доступной техники."
        task_node = task_nodes.get(task.task_id)
        if task_node is None:
            return f"Не назначена: для скважины {task.destination_uwi} не найден узел дорожного графа."

        compatible_units = 0
        with_path_units = 0
        over_limit_units = 0
        min_projected = None
        min_projected_unit = None
        best_distance = None
        best_eta = None

        for unit in unit_state.values():
            status = True
            if has_rules:
                status = compatibility_status(task.task_type, unit.unit_type, compat, compat_norm)
                if status is False or status is None:
                    if settings.compatibility_strict:
                        continue
            compatible_units += 1

            route = self.routing.route_between_nodes_or_none(
                unit.node_id, task_node, speed_kmph=unit.speed_kmph
            )
            if route is None:
                continue
            with_path_units += 1

            distance_km = route["distance_km"]
            travel_minutes = route["time_minutes"]
            if best_distance is None or distance_km < best_distance:
                best_distance = distance_km
            if best_eta is None or travel_minutes < best_eta:
                best_eta = travel_minutes

            if unit_minutes.get(unit.wialon_id, 0.0) > 0 and max_total_time_minutes:
                projected = unit_minutes[unit.wialon_id] + travel_minutes + task.duration_hours * 60.0
                if projected > max_total_time_minutes:
                    over_limit_units += 1
                    if min_projected is None or projected < min_projected:
                        min_projected = projected
                        min_projected_unit = unit.wialon_id
                    continue
            # If we got here, at least one feasible candidate exists.
            return "Не назначена: в текущем шаге выбрана другая заявка с лучшим суммарным скором."

        if compatible_units == 0:
            return (
                f"Не назначена: нет совместимой техники для типа работ '{task.task_type}' "
                f"(режим strict, проверено {total_units} ед. техники)."
            )
        if with_path_units == 0:
            return (
                f"Не назначена: для {compatible_units} совместимых машин не найден маршрут по графу "
                f"до скважины {task.destination_uwi}."
            )
        if over_limit_units == with_path_units and max_total_time_minutes:
            min_text = (
                f"; минимально требуется {int(round(min_projected))} мин (машина {min_projected_unit})"
                if min_projected is not None
                else ""
            )
            return (
                f"Не назначена: все {with_path_units} кандидатов превышают лимит выезда "
                f"{max_total_time_minutes} мин{min_text}."
            )

        extra = []
        if best_distance is not None:
            extra.append(f"лучший маршрут {best_distance:.2f} км")
        if best_eta is not None:
            extra.append(f"лучший ETA {best_eta} мин")
        suffix = f" ({', '.join(extra)})" if extra else ""
        return (
            "Не назначена: нет кандидата, который одновременно проходит ограничения "
            f"совместимости/графа/времени{suffix}."
        )

    def _group_failure_reason(
        self,
        group: List[Task],
        wells_map,
        task_nodes: Dict[str, int],
        unit_state: Dict[int, object],
        has_rules: bool,
        compat: Dict[str, Set[str]],
        compat_norm: Dict[str, Set[str]],
        max_total_time_minutes: int,
        unit_minutes: Dict[int, float],
    ) -> str:
        total_units = len(unit_state)
        if total_units == 0:
            return "Не назначена: в снапшоте нет доступной техники."

        ordered = self._nearest_neighbor_order(group, wells_map)
        compatible_units = 0
        path_ok_units = 0
        limit_block_units = 0
        min_projected = None
        no_path_units = 0

        for unit_id, unit in unit_state.items():
            if not self._is_unit_compatible(unit, ordered, has_rules, compat, compat_norm):
                continue
            compatible_units += 1

            current_node = unit.node_id
            used_minutes = unit_minutes.get(unit_id, 0.0)
            projected = used_minutes
            path_failed = False
            for task in ordered:
                task_node = task_nodes.get(task.task_id)
                if task_node is None:
                    path_failed = True
                    break
                route = self.routing.route_between_nodes_or_none(
                    current_node, task_node, speed_kmph=unit.speed_kmph
                )
                if route is None:
                    path_failed = True
                    break
                projected += route["time_minutes"] + task.duration_hours * 60.0
                current_node = task_node
            if path_failed:
                no_path_units += 1
                continue

            path_ok_units += 1
            if max_total_time_minutes and used_minutes > 0 and projected > max_total_time_minutes:
                limit_block_units += 1
                if min_projected is None or projected < min_projected:
                    min_projected = projected

        task_ids = ",".join(t.task_id for t in ordered)
        if compatible_units == 0:
            return (
                f"Не назначена группа [{task_ids}]: нет совместимой техники "
                f"(strict, проверено {total_units} ед.)."
            )
        if path_ok_units == 0:
            return (
                f"Не назначена группа [{task_ids}]: для {compatible_units} совместимых машин "
                "не найден полный маршрут по графу."
            )
        if limit_block_units == path_ok_units and max_total_time_minutes:
            min_text = (
                f"; минимально требуется {int(round(min_projected))} мин"
                if min_projected is not None
                else ""
            )
            return (
                f"Не назначена группа [{task_ids}]: все кандидаты превышают лимит выезда "
                f"{max_total_time_minutes} мин{min_text}."
            )
        if no_path_units > 0 and limit_block_units > 0:
            return (
                f"Не назначена группа [{task_ids}]: часть машин без маршрута по графу "
                f"({no_path_units}), часть превышает лимит времени ({limit_block_units})."
            )
        in_limit_units = max(0, path_ok_units - limit_block_units)
        return (
            f"Не назначена группа [{task_ids}]: нет кандидата, который одновременно "
            "проходит совместимость, граф и лимиты времени. "
            f"Проверено: совместимых {compatible_units}, с полным маршрутом {path_ok_units}, в лимите {in_limit_units}."
        )

    def _can_add(
        self,
        group,
        candidate,
        wells_map,
        max_total_time_minutes,
        max_detour_ratio,
        has_rules: bool,
        compat: Dict[str, Set[str]],
        compat_norm: Dict[str, Set[str]],
    ):
        new_group = group + [candidate]
        if not self._has_compatible_unit(new_group, has_rules, compat, compat_norm):
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

    def _has_compatible_unit(
        self,
        tasks: List[Task],
        has_rules: bool,
        compat: Dict[str, Set[str]],
        compat_norm: Dict[str, Set[str]],
    ) -> bool:
        if not has_rules:
            return True
        if not settings.compatibility_strict:
            return True
        units = self.repo.units_snapshot()
        if not units:
            return False
        for unit in units:
            ok = True
            for task in tasks:
                status = compatibility_status(task.task_type, unit.unit_type, compat, compat_norm)
                if status is False or status is None:
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
                route = self.routing.route_between_points_or_none(well_a.lon, well_a.lat, well_b.lon, well_b.lat)
                if route is None:
                    return False
                travel_minutes = route["time_minutes"]
                arrival_time = current_time + timedelta(minutes=travel_minutes)

            start_time = max(arrival_time, task.planned_start)
            current_time = start_time + timedelta(hours=task.duration_hours)
        return True

    def _group_route_distance(self, group, wells_map):
        ordered = self._nearest_neighbor_order(group, wells_map)
        distance = 0.0
        for a, b in zip(ordered, ordered[1:]):
            well_a = wells_map[a.destination_uwi]
            well_b = wells_map[b.destination_uwi]
            route = self.routing.route_between_points_or_none(well_a.lon, well_a.lat, well_b.lon, well_b.lat)
            if route is None:
                return 1e9
            distance += route["distance_km"]
        return max(distance, 0.1)

    def _group_baseline_distance(self, group, wells_map):
        total_distance = 0.0
        units = self.repo.units_snapshot()
        for task in group:
            well = wells_map[task.destination_uwi]
            best_dist = None
            for unit in units:
                route = self.routing.route_between_points_or_none(unit.pos_x, unit.pos_y, well.lon, well.lat)
                if route is None:
                    continue
                d = route["distance_km"]
                if best_dist is None or d < best_dist:
                    best_dist = d
            if best_dist is None:
                return 0.0
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

    def _choose_unit_for_group(
        self,
        group,
        wells_map,
        unit_state: Dict[int, object],
        has_rules: bool,
        compat: Dict[str, Set[str]],
        compat_norm: Dict[str, Set[str]],
        max_total_time_minutes: int,
        unit_minutes: Dict[int, float],
    ) -> Tuple[int | None, List[AssignmentItem] | None, float, str]:
        ordered = self._nearest_neighbor_order(group, wells_map)
        best_cost = None
        best_unit_id = None
        best_plan = None
        best_total_minutes = 0.0
        fail_reasons: Set[str] = set()

        for unit_id, unit in unit_state.items():
            if not self._is_unit_compatible(unit, ordered, has_rules, compat, compat_norm):
                fail_reasons.add("no_compatible_unit")
                continue

            plan_items = []
            current_node = unit.node_id
            current_time = unit.available_at
            used_minutes = unit_minutes.get(unit_id, 0.0)
            total_cost = 0.0
            total_minutes = 0.0
            failed = False
            fail_reason = ""

            for task in ordered:
                well = wells_map[task.destination_uwi]
                task_node = self.routing.node_index.nearest(well.lon, well.lat)
                route = self.routing.route_between_nodes_or_none(current_node, task_node, speed_kmph=unit.speed_kmph)
                if route is None:
                    failed = True
                    fail_reason = f"no_path:{task.task_id}"
                    break
                distance_km = route["distance_km"]
                travel_minutes = route["time_minutes"]
                projected = used_minutes + travel_minutes + task.duration_hours * 60.0
                if max_total_time_minutes and used_minutes > 0 and projected > max_total_time_minutes:
                    failed = True
                    fail_reason = f"max_total_time:{task.task_id}"
                    break
                compat_penalty = 0.0
                if has_rules and not settings.compatibility_strict:
                    status = compatibility_status(task.task_type, unit.unit_type, compat, compat_norm)
                    if status is False:
                        compat_penalty = settings.compatibility_penalty

                score_result = score_task(
                    task=task,
                    distance_km=distance_km,
                    travel_minutes=travel_minutes,
                    unit_available_at=current_time,
                    compatibility_penalty=compat_penalty,
                )

                total_cost += score_result.cost
                total_minutes += travel_minutes + task.duration_hours * 60.0
                used_minutes = projected
                plan_items.append(
                    AssignmentItem(
                        task_id=task.task_id,
                        wialon_id=unit.wialon_id,
                        eta_minutes=score_result.eta_minutes,
                        distance_km=distance_km,
                        score=score_result.score,
                        reason=score_result.reason,
                        planned_duration_hours=task.duration_hours,
                        planned_start=task.planned_start,
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
                best_total_minutes = total_minutes

        if best_unit_id is None or best_plan is None:
            reason = "; ".join(sorted(fail_reasons)) if fail_reasons else "no_feasible_unit"
            return None, None, 0.0, reason
        return best_unit_id, best_plan, best_total_minutes, ""

    def _is_unit_compatible(
        self,
        unit,
        tasks: List[Task],
        has_rules: bool,
        compat: Dict[str, Set[str]],
        compat_norm: Dict[str, Set[str]],
    ) -> bool:
        if not has_rules:
            return True
        if not settings.compatibility_strict:
            return True
        for task in tasks:
            status = compatibility_status(task.task_type, unit.unit_type, compat, compat_norm)
            if status is False or status is None:
                return False
        return True
