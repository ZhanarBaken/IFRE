from __future__ import annotations

from typing import List

from app.data.repositories.base import BaseRepository
from app.models.schemas import RecommendationUnit, Task
from app.services.duration_forecast import DurationForecaster
from app.services.fleet_state import FleetStateService
from app.services.routing import RoutingService
from app.services.scoring import score_task


class RecommendationService:
    def __init__(self, repo: BaseRepository, routing: RoutingService) -> None:
        self.repo = repo
        self.routing = routing
        self.fleet_state = FleetStateService(repo, routing)
        self.duration_forecaster = DurationForecaster(repo)

    def _is_compatible(self, task_type: str, unit_type: str) -> bool:
        rules = self.repo.compatibility()
        if not rules:
            return True
        for rule in rules:
            if rule.task_type == task_type and rule.unit_type == unit_type:
                return True
        return False

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
        units_state = self.fleet_state.build_state()

        if mode_norm == "baseline":
            return self._recommend_baseline(units_state, well, task_type, planned_start)

        candidates: List[RecommendationUnit] = []
        task_for_score = self._task_for_scoring(
            task_id, priority, destination_uwi, task_type, planned_start, duration_hours
        )

        unit_availability = {u.wialon_id: u.available_at.isoformat() for u in units_state.values()}

        for unit in units_state.values():
            if task_type and not self._is_compatible(task_type, unit.unit_type):
                continue

            route = self.routing.route_between_points(unit.lon, unit.lat, well.lon, well.lat)
            distance_km = route["distance_km"]
            travel_minutes = int(round(distance_km / unit.speed_kmph * 60.0))
            if exclude_busy and planned_start is not None and unit.available_at > planned_start:
                continue
            score_result = score_task(
                task=task_for_score,
                distance_km=distance_km,
                travel_minutes=travel_minutes,
                unit_available_at=unit.available_at,
            )

            candidates.append(
                RecommendationUnit(
                    wialon_id=unit.wialon_id,
                    name=unit.name,
                    eta_minutes=int(round(travel_minutes)),
                    distance_km=round(distance_km, 2),
                    score=score_result.score,
                    reason=score_result.reason,
                )
            )

        candidates.sort(key=lambda x: x.score, reverse=True)
        return candidates[:3]

    def _well(self, uwi: str):
        wells = self.repo.wells_by_uwi([uwi])
        if not wells:
            raise ValueError(f"Well not found: {uwi}")
        return wells[0]

    def _infer_task_type(self, task_id: str) -> str | None:
        task = self.repo.task_by_id(task_id)
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
        task = self.repo.task_by_id(task_id)
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

    def _recommend_baseline(self, units_state, well, task_type: str | None, planned_start):
        candidates: List[RecommendationUnit] = []
        for unit in units_state.values():
            if task_type and not self._is_compatible(task_type, unit.unit_type):
                continue
            route = self.routing.route_between_points(unit.lon, unit.lat, well.lon, well.lat)
            distance_km = route["distance_km"]
            travel_minutes = int(round(distance_km / unit.speed_kmph * 60.0))
            wait_minutes = 0
            if planned_start is not None:
                wait_minutes = max(0, int((unit.available_at - planned_start).total_seconds() / 60.0))
            reason = "baseline: nearest by distance"
            if wait_minutes > 0:
                reason = f"{reason}, waits {wait_minutes} min"
            score = 1.0 / (1.0 + distance_km)
            candidates.append(
                RecommendationUnit(
                    wialon_id=unit.wialon_id,
                    name=unit.name,
                    eta_minutes=int(round(travel_minutes)),
                    distance_km=round(distance_km, 2),
                    score=round(score, 3),
                    reason=reason,
                )
            )
        candidates.sort(key=lambda x: x.distance_km)
        return candidates[:3]
