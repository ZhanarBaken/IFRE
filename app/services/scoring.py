from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta

from app.core.config import settings
from app.models.schemas import Task


@dataclass
class ScoreResult:
    score: float
    cost: float
    reason: str
    eta_minutes: int
    wait_minutes: int
    late_minutes: int
    start_time: datetime
    end_time: datetime


def priority_deadline_hours(priority: str) -> int:
    return {"high": 2, "medium": 5, "low": 12}.get(priority, 5)


def priority_weight(priority: str) -> float:
    return {"high": 0.55, "medium": 0.35, "low": 0.10}.get(priority, 0.35)


def shift_window(dt: datetime) -> tuple[datetime, datetime]:
    day_start = time(8, 0)
    day_end = time(20, 0)
    if day_start <= dt.time() < day_end:
        start = datetime.combine(dt.date(), day_start)
        end = datetime.combine(dt.date(), day_end)
    else:
        if dt.time() >= day_end:
            start = datetime.combine(dt.date(), day_end)
            end = datetime.combine(dt.date() + timedelta(days=1), day_start)
        else:
            start = datetime.combine(dt.date() - timedelta(days=1), day_end)
            end = datetime.combine(dt.date(), day_start)
    return start, end


def score_task(
    task: Task,
    distance_km: float,
    travel_minutes: int,
    unit_available_at: datetime,
    compatibility_penalty: float = 0.0,
) -> ScoreResult:
    planned_start = task.planned_start
    deadline = planned_start + timedelta(hours=priority_deadline_hours(task.priority))
    arrival_time = unit_available_at + timedelta(minutes=travel_minutes)
    start_time = max(arrival_time, planned_start)
    wait_minutes = max(0, int((planned_start - arrival_time).total_seconds() / 60.0))
    late_minutes = max(0, int((start_time - deadline).total_seconds() / 60.0))
    prio_weight = priority_weight(task.priority)

    # Cost model
    cost = (
        settings.score_w_distance * distance_km
        + settings.score_w_eta * travel_minutes
        + settings.score_w_wait * wait_minutes * prio_weight
        + settings.score_w_late * late_minutes * prio_weight
    )
    if compatibility_penalty:
        cost += compatibility_penalty

    score = 1.0 / (1.0 + cost)
    reason = (
        f"dist {distance_km:.2f} km, eta {travel_minutes} min, "
        f"wait {wait_minutes} min, late {late_minutes} min, prio {task.priority}"
    )
    if compatibility_penalty:
        reason = f"{reason}, compat_penalty {compatibility_penalty:.2f}"
    end_time = start_time + timedelta(hours=task.duration_hours)
    return ScoreResult(
        score=round(score, 3),
        cost=cost,
        reason=reason,
        eta_minutes=travel_minutes,
        wait_minutes=wait_minutes,
        late_minutes=late_minutes,
        start_time=start_time,
        end_time=end_time,
    )
