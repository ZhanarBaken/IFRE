from __future__ import annotations

from datetime import datetime
from typing import List

from app.core.config import settings
from app.data.repositories.base import BaseRepository
from app.models.schemas import RecommendationUnit, Task
from app.services.compatibility import build_compat_index, compatibility_status
from app.services.duration_forecast import DurationForecaster
from app.services.fleet_state import FleetStateService
from app.services.reason_ai import ReasonAIService
from app.services.routing import RoutingService
from app.services.scoring import score_task, score_to_points


class RecommendationService:
    def __init__(self, repo: BaseRepository, routing: RoutingService) -> None:
        self.repo = repo
        self.routing = routing
        self.fleet_state = FleetStateService(repo, routing)
        self.duration_forecaster = DurationForecaster(repo)
        self.reason_ai = ReasonAIService()

    def recommend(
        self,
        task_id: str,
        priority: str,
        destination_uwi: str,
        task_type: str | None,
        planned_start,
        duration_hours,
        mode: str | None = None,
        exclude_busy: bool = False,
    ):
        mode_norm = (mode or "optimized").lower()
        task_type = task_type or self._infer_task_type(task_id)
        if task_type == "unknown":
            task_type = None
        well = self._well(destination_uwi)
        units_state = self.fleet_state.build_state(anchor_time=planned_start)
        rules = self.repo.compatibility()
        has_rules = bool(rules)
        compat = {}
        compat_norm = {}
        if has_rules:
            compat, compat_norm = build_compat_index(rules)

        if mode_norm == "baseline":
            return self._recommend_baseline(units_state, well, task_type, planned_start, has_rules, compat, compat_norm)

        candidates: list[dict] = []
        task_for_score = self._task_for_scoring(
            task_id, priority, destination_uwi, task_type, planned_start, duration_hours
        )

        for unit in units_state.values():
            status = True
            if has_rules and task_type:
                status = compatibility_status(task_type, unit.unit_type, compat, compat_norm)
                if status is False or status is None:
                    if settings.compatibility_strict:
                        continue

            route = self.routing.route_between_points_or_none(unit.lon, unit.lat, well.lon, well.lat)
            if route is None:
                continue
            distance_km = route["distance_km"]
            travel_minutes = int(round(distance_km / unit.speed_kmph * 60.0))
            if exclude_busy and planned_start is not None and unit.available_at > planned_start:
                continue
            compat_penalty = 0.0
            if has_rules and status is False and not settings.compatibility_strict:
                compat_penalty = settings.compatibility_penalty
            score_result = score_task(
                task=task_for_score,
                distance_km=distance_km,
                travel_minutes=travel_minutes,
                unit_available_at=unit.available_at,
                compatibility_penalty=compat_penalty,
            )

            release_minutes = self._release_minutes(unit.available_at, planned_start)
            candidates.append(
                {
                    "wialon_id": unit.wialon_id,
                    "name": unit.name,
                    "eta_minutes": int(round(travel_minutes)),
                    "distance_km": round(distance_km, 2),
                    "score": score_result.score,
                    "wait_minutes": score_result.wait_minutes,
                    "late_minutes": score_result.late_minutes,
                    "release_minutes": release_minutes,
                    "compat_penalty": compat_penalty,
                    "compatible": not (has_rules and status is False),
                }
            )

        candidates.sort(key=lambda x: x["score"], reverse=True)
        top_raw = candidates[:3]
        best_distance = top_raw[0]["distance_km"] if top_raw else None
        top: List[RecommendationUnit] = []
        for idx, cand in enumerate(top_raw):
            reason = self._build_recommendation_reason(cand, idx, best_distance)
            top.append(
                RecommendationUnit(
                    wialon_id=cand["wialon_id"],
                    name=cand["name"],
                    eta_minutes=cand["eta_minutes"],
                    distance_km=cand["distance_km"],
                    score=cand["score"],
                    reason=reason,
                )
            )
        self._rewrite_reasons(top, context="recommendations")
        return top

    def _well(self, uwi: str):
        wells = self.repo.wells_by_uwi([uwi])
        if not wells:
            raise ValueError(f"Well not found: {uwi}")
        return wells[0]

    def _infer_task_type(self, task_id: str) -> str | None:
        try:
            task = self.repo.task_by_id(task_id)
        except Exception:
            return None
        return task.task_type if task else None

    def _task_for_scoring(
        self,
        task_id: str,
        priority: str,
        destination_uwi: str,
        task_type: str | None,
        planned_start,
        duration_hours,
    ) -> Task:
        try:
            task = self.repo.task_by_id(task_id)
        except Exception:
            task = None
        if planned_start is None:
            from datetime import datetime

            planned_start = datetime.utcnow()
        duration_hours = self.duration_forecaster.ensure_duration(task_type, duration_hours)
        if task:
            task.priority = priority
            task.destination_uwi = destination_uwi
            task.planned_start = planned_start
            task.duration_hours = duration_hours
            if task_type:
                task.task_type = task_type
            return task

        return Task(
            task_id=task_id,
            priority=priority,
            destination_uwi=destination_uwi,
            planned_start=planned_start,
            duration_hours=duration_hours,
            task_type=task_type or "unknown",
        )

    def _recommend_baseline(
        self,
        units_state,
        well,
        task_type: str | None,
        planned_start,
        has_rules: bool,
        compat,
        compat_norm,
    ):
        candidates: list[dict] = []
        for unit in units_state.values():
            if has_rules and task_type:
                status = compatibility_status(task_type, unit.unit_type, compat, compat_norm)
                if status is False or status is None:
                    if settings.compatibility_strict:
                        continue
            route = self.routing.route_between_points_or_none(unit.lon, unit.lat, well.lon, well.lat)
            if route is None:
                continue
            distance_km = route["distance_km"]
            travel_minutes = int(round(distance_km / unit.speed_kmph * 60.0))
            wait_minutes = 0
            if planned_start is not None:
                wait_minutes = max(0, int((unit.available_at - planned_start).total_seconds() / 60.0))
            score = score_to_points(distance_km)
            candidates.append(
                {
                    "wialon_id": unit.wialon_id,
                    "name": unit.name,
                    "eta_minutes": int(round(travel_minutes)),
                    "distance_km": round(distance_km, 2),
                    "score": score,
                    "wait_minutes": wait_minutes,
                }
            )
        candidates.sort(key=lambda x: x["distance_km"])
        top_raw = candidates[:3]
        best_distance = top_raw[0]["distance_km"] if top_raw else None
        top: List[RecommendationUnit] = []
        for idx, cand in enumerate(top_raw):
            reason = self._build_baseline_reason(cand, idx, best_distance)
            top.append(
                RecommendationUnit(
                    wialon_id=cand["wialon_id"],
                    name=cand["name"],
                    eta_minutes=cand["eta_minutes"],
                    distance_km=cand["distance_km"],
                    score=cand["score"],
                    reason=reason,
                )
            )
        self._rewrite_reasons(top, context="recommendations_baseline")
        return top

    def _rewrite_reasons(self, units: List[RecommendationUnit], context: str) -> None:
        if not units:
            return
        rewritten = self.reason_ai.rewrite_many([item.reason or "" for item in units], context=context)
        if len(rewritten) != len(units):
            return
        for item, reason in zip(units, rewritten):
            item.reason = reason

    def _release_minutes(self, unit_available_at: datetime, planned_start: datetime | None) -> int:
        if planned_start is None:
            return 0
        return max(0, int((unit_available_at - planned_start).total_seconds() / 60.0))

    def _build_recommendation_reason(self, c: dict, rank: int, best_distance: float | None) -> str:
        distance = float(c["distance_km"])
        eta = int(c["eta_minutes"])
        wait = int(c["wait_minutes"])
        late = int(c["late_minutes"])
        release = int(c["release_minutes"])
        compat_penalty = float(c["compat_penalty"])
        compatible = bool(c["compatible"])
        delta_km = 0.0 if best_distance is None else max(0.0, distance - best_distance)

        if rank == 0 and wait == 0 and compatible and compat_penalty <= 0:
            base = "ближайшая свободная, совместима по типу работ"
        elif wait > 0:
            base = f"занята, освободится через {release} мин, затем прибудет за {eta} мин"
        else:
            base = f"свободна, ETA {eta} мин"

        parts = [base]
        if rank > 0 and delta_km > 0:
            parts.append(f"дальше лидера на {delta_km:.1f} км")
        elif rank == 0 and distance > 0:
            parts.append(f"маршрут {distance:.1f} км")
        if late > 0:
            parts.append(f"ожидаемое опоздание по SLA {late} мин")
        if compat_penalty > 0:
            parts.append("допущена с штрафом за несовместимость")
        return ", ".join(parts)

    def _build_baseline_reason(self, c: dict, rank: int, best_distance: float | None) -> str:
        distance = float(c["distance_km"])
        wait = int(c["wait_minutes"])
        delta_km = 0.0 if best_distance is None else max(0.0, distance - best_distance)
        if rank == 0:
            base = "baseline: ближайшая по расстоянию"
        else:
            base = f"baseline: дальше лидера на {delta_km:.1f} км"
        if wait > 0:
            return f"{base}, ожидание {wait} мин"
        return base
