from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Set

from app.core.config import settings
from app.data.repositories.base import BaseRepository
from app.models.schemas import Task, TaskFilters
from app.services.compatibility import build_compat_index, compatibility_status
from app.services.duration_forecast import DurationForecaster
from app.services.reason_ai import ReasonAIService
from app.services.routing import RoutingService
from app.utils.geo import distance_km


class MultitaskService:
    def __init__(self, repo: BaseRepository, routing: RoutingService) -> None:
        self.repo = repo
        self.routing = routing
        self.duration_forecaster = DurationForecaster(repo)
        self.reason_ai = ReasonAIService()

    def evaluate(self, task_ids: List[str] | None, filters: TaskFilters | None, max_total_time_minutes: int, max_detour_ratio: float):
        tasks = self._load_tasks(task_ids, filters)
        if not tasks:
            return self._empty()
        self.duration_forecaster.fill_missing(tasks)

        wells_map = self._prefetch_wells(tasks)
        rules = self.repo.compatibility()
        has_rules = bool(rules)
        compat: Dict[str, Set[str]] = {}
        compat_norm: Dict[str, Set[str]] = {}
        if has_rules:
            compat, compat_norm = build_compat_index(rules)
        groups = self._cluster_tasks(
            tasks, wells_map, max_total_time_minutes, max_detour_ratio, has_rules, compat, compat_norm
        )
        baseline = self._baseline(tasks, wells_map)
        if baseline is None:
            return self._empty(reason="no_path")
        baseline_distance_km, baseline_time_minutes = baseline
        metrics = self._estimate_group_metrics(groups, wells_map)
        if metrics is None:
            return self._empty(reason="no_path")
        total_distance_km, total_time_minutes = metrics

        if total_distance_km <= 0:
            return self._empty()

        savings_percent = max(0.0, (1.0 - total_distance_km / baseline_distance_km) * 100.0)
        strategy = "separate" if len(groups) == len(tasks) else "mixed"
        if len(groups) == 1 and len(tasks) > 1:
            strategy = "single_unit"

        reason = self._build_reason(
            groups=groups,
            tasks=tasks,
            wells_map=wells_map,
            strategy=strategy,
            total_distance_km=total_distance_km,
            total_time_minutes=total_time_minutes,
            baseline_distance_km=baseline_distance_km,
            baseline_time_minutes=baseline_time_minutes,
            savings_percent=savings_percent,
            max_total_time_minutes=max_total_time_minutes,
            max_detour_ratio=max_detour_ratio,
        )
        reason = self.reason_ai.rewrite_one(reason, context=f"multitask_{strategy}")
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

    def _baseline(self, tasks, wells_map) -> tuple[float, float] | None:
        total_distance = 0.0
        total_time = 0.0
        units = self.repo.units_snapshot()
        if not units:
            raise RuntimeError("units_snapshot is empty")
        for task in tasks:
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
                return None
            total_distance += best_dist
            total_time += best_dist / settings.avg_speed_kmph * 60.0 + task.duration_hours * 60.0
        return max(total_distance, 0.01), max(total_time, 1.0)

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
        remaining = tasks[:]
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
        if route_distance is None:
            return False
        baseline_distance, _baseline_time = self._group_baseline(new_group, wells_map)
        if baseline_distance <= 0:
            return False
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

    def _group_baseline(self, group, wells_map):
        baseline = self._baseline(group, wells_map)
        if baseline is None:
            return 0.0, 0.0
        return baseline

    def _group_route_distance(self, group, wells_map):
        ordered = self._nearest_neighbor_order(group, wells_map)
        distance = 0.0
        units = self.repo.units_snapshot()
        if not units:
            return None
        first_well = wells_map[ordered[0].destination_uwi]
        best_start = None
        for unit in units:
            route = self.routing.route_between_points_or_none(unit.pos_x, unit.pos_y, first_well.lon, first_well.lat)
            if route is None:
                continue
            d = route["distance_km"]
            if best_start is None or d < best_start:
                best_start = d
        if best_start is None:
            return None
        distance += best_start
        for a, b in zip(ordered, ordered[1:]):
            well_a = wells_map[a.destination_uwi]
            well_b = wells_map[b.destination_uwi]
            route = self.routing.route_between_points_or_none(well_a.lon, well_a.lat, well_b.lon, well_b.lat)
            if route is None:
                return None
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
            if distance is None:
                return None
            total_distance += distance
            total_time += distance / settings.avg_speed_kmph * 60.0 + sum(
                t.duration_hours * 60.0 for t in group
            )
        return total_distance, total_time

    def _empty(self, reason: str = "no tasks"):
        reason_text = self.reason_ai.rewrite_one(reason, context="multitask_empty")
        return {
            "groups": [],
            "strategy_summary": "separate",
            "total_distance_km": 0.0,
            "total_time_minutes": 0,
            "baseline_distance_km": 0.0,
            "baseline_time_minutes": 0,
            "savings_percent": 0.0,
            "reason": reason_text,
        }

    def _build_reason(
        self,
        groups,
        tasks,
        wells_map,
        strategy: str,
        total_distance_km: float,
        total_time_minutes: float,
        baseline_distance_km: float,
        baseline_time_minutes: float,
        savings_percent: float,
        max_total_time_minutes: int,
        max_detour_ratio: float,
    ) -> str:
        merged_groups = [group for group in groups if len(group) > 1]
        merged_labels = [",".join(task.task_id for task in group) for group in merged_groups]
        min_pair_km = self._min_pair_distance_km(tasks, wells_map)
        saved_km = baseline_distance_km - total_distance_km
        saved_min = baseline_time_minutes - total_time_minutes

        if strategy == "single_unit":
            ids = ",".join(task.task_id for task in groups[0]) if groups else ""
            diameter = self._group_diameter_km(groups[0], wells_map) if groups else 0.0
            detour_percent = max(0.0, (max_detour_ratio - 1.0) * 100.0)
            return (
                f"все заявки {ids} расположены компактно (радиус около {diameter:.1f} км), "
                f"поэтому выполнены одним выездом; крюк в пределах лимита {detour_percent:.0f}%. "
                f"Экономия {saved_km:.1f} км и {saved_min:.0f} мин ({savings_percent:.1f}% к baseline)."
            )

        if strategy == "mixed":
            merged_text = "; ".join(merged_labels) if merged_labels else "часть заявок"
            first = merged_groups[0] if merged_groups else None
            first_diameter = self._group_diameter_km(first, wells_map) if first else 0.0
            return (
                f"предложена группировка заявок {merged_text} (скважины рядом: около {first_diameter:.1f} км). "
                f"Остальные не объединены — крюк превысил бы ограничения "
                f"(max_detour_ratio={max_detour_ratio}, max_total_time_minutes={max_total_time_minutes}). "
                f"Потенциальная экономия при успешном назначении: {saved_km:.1f} км и {saved_min:.0f} мин ({savings_percent:.1f}% к baseline)."
            )

        # separate
        min_pair_text = f"{min_pair_km:.1f} км" if min_pair_km is not None else "н/д"
        return (
            f"задачи оставлены раздельно: минимальное расстояние между парами {min_pair_text}, "
            f"а объединение не проходит по ограничениям "
            f"max_detour_ratio={max_detour_ratio} и max_total_time_minutes={max_total_time_minutes}. "
            f"Экономия относительно baseline отсутствует ({savings_percent:.1f}%)."
        )

    def _min_pair_distance_km(self, tasks, wells_map) -> float | None:
        if len(tasks) < 2:
            return 0.0
        min_dist = None
        for i in range(len(tasks)):
            for j in range(i + 1, len(tasks)):
                a = wells_map[tasks[i].destination_uwi]
                b = wells_map[tasks[j].destination_uwi]
                d = distance_km(a.lon, a.lat, b.lon, b.lat)
                if min_dist is None or d < min_dist:
                    min_dist = d
        return min_dist

    def _group_diameter_km(self, group, wells_map) -> float:
        if not group or len(group) < 2:
            return 0.0
        max_dist = 0.0
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                a = wells_map[group[i].destination_uwi]
                b = wells_map[group[j].destination_uwi]
                d = distance_km(a.lon, a.lat, b.lon, b.lat)
                if d > max_dist:
                    max_dist = d
        return max_dist
