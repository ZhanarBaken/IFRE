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


def score_to_points(cost: float) -> float:
    # Human-readable score in 0..100 range (higher is better).
    scale = max(0.1, settings.score_points_scale)
    points = 100.0 / (1.0 + (cost / scale))
    return round(points, 1)


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

    # Cost model (all factors are explicit to keep reason explainable)
    distance_cost = settings.score_w_distance * distance_km
    eta_cost = settings.score_w_eta * travel_minutes
    wait_cost = settings.score_w_wait * wait_minutes * prio_weight
    late_cost = settings.score_w_late * late_minutes * prio_weight
    cost = distance_cost + eta_cost + wait_cost + late_cost
    if compatibility_penalty:
        cost += compatibility_penalty

    score = score_to_points(cost)
    reason = (
        f"факторы: расстояние {distance_km:.2f} км (вклад {distance_cost:.2f}), "
        f"время в пути {travel_minutes} мин (вклад {eta_cost:.2f}), "
        f"ожидание {wait_minutes} мин (вклад {wait_cost:.2f}), "
        f"SLA-опоздание {late_minutes} мин (вклад {late_cost:.2f}), "
        f"приоритет {task.priority}. "
        f"план {planned_start.isoformat()}, дедлайн {deadline.isoformat()}, "
        f"доступность техники {unit_available_at.isoformat()}, старт {start_time.isoformat()}."
    )
    if compatibility_penalty:
        reason = f"{reason} штраф совместимости {compatibility_penalty:.2f}."
    reason = f"{reason} итоговая стоимость {cost:.2f}, балл {score:.1f}/100."
    end_time = start_time + timedelta(hours=task.duration_hours)
    return ScoreResult(
        score=score,
        cost=cost,
        reason=reason,
        eta_minutes=travel_minutes,
        wait_minutes=wait_minutes,
        late_minutes=late_minutes,
        start_time=start_time,
        end_time=end_time,
    )
