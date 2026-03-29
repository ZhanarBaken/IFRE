"""Tests for scoring, duration forecast, and 2-opt route optimizer."""
from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from app.models.schemas import Task
from app.services.scoring import priority_deadline_hours, score_task, score_to_points
from app.services.duration_forecast import StatsDurationModel, DEFAULT_DURATION_HOURS
from app.services.route_optimizer import two_opt_order


# ── Helpers ────────────────────────────────────────────────────────────────

def make_task(
    task_id: str = "t1",
    priority: str = "medium",
    duration_hours: float = 2.0,
    task_type: str = "TypeA",
    planned_start: datetime | None = None,
    destination_uwi: str = "W1",
) -> Task:
    return Task(
        task_id=task_id,
        priority=priority,
        destination_uwi=destination_uwi,
        planned_start=planned_start or datetime(2025, 9, 1, 8, 0),
        duration_hours=duration_hours,
        task_type=task_type,
    )


# ── Scoring ────────────────────────────────────────────────────────────────

class TestScoring:
    def test_priority_deadlines(self):
        assert priority_deadline_hours("high") == 2
        assert priority_deadline_hours("medium") == 5
        assert priority_deadline_hours("low") == 12
        assert priority_deadline_hours("unknown") == 5  # default

    def test_score_to_points_zero_cost(self):
        # cost=0 → 100 points
        assert score_to_points(0.0) == 100.0

    def test_score_to_points_decreasing(self):
        # Higher cost → lower score
        assert score_to_points(5.0) > score_to_points(50.0)

    def test_score_task_no_late(self):
        task = make_task(priority="medium", planned_start=datetime(2025, 9, 1, 10, 0))
        unit_available = datetime(2025, 9, 1, 9, 0)
        result = score_task(task, distance_km=10.0, travel_minutes=30, unit_available_at=unit_available)
        assert result.late_minutes == 0
        assert result.score > 0
        assert result.start_time == datetime(2025, 9, 1, 10, 0)  # wait for planned_start

    def test_score_task_with_late_penalty(self):
        task = make_task(priority="high", planned_start=datetime(2025, 9, 1, 8, 0))
        # unit arrives 3 hours after deadline (high deadline = 2h → deadline at 10:00)
        unit_available = datetime(2025, 9, 1, 13, 0)
        result = score_task(task, distance_km=5.0, travel_minutes=0, unit_available_at=unit_available)
        assert result.late_minutes > 0
        # Late result has lower score than on-time
        on_time = score_task(task, distance_km=5.0, travel_minutes=0,
                             unit_available_at=datetime(2025, 9, 1, 8, 0))
        assert result.score < on_time.score

    def test_score_task_end_time(self):
        task = make_task(duration_hours=3.0, planned_start=datetime(2025, 9, 1, 8, 0))
        result = score_task(task, distance_km=0.0, travel_minutes=0,
                            unit_available_at=datetime(2025, 9, 1, 8, 0))
        assert result.end_time == datetime(2025, 9, 1, 11, 0)


# ── Duration Forecast ──────────────────────────────────────────────────────

class TestStatsDurationModel:
    def test_median_by_type(self):
        tasks = [
            make_task(task_type="A", duration_hours=2.0),
            make_task(task_type="A", duration_hours=4.0),
            make_task(task_type="B", duration_hours=1.0),
        ]
        model = StatsDurationModel.from_tasks(tasks)
        assert model.predict("A") == 3.0   # median(2, 4)
        assert model.predict("B") == 1.0

    def test_fallback_to_global_median(self):
        tasks = [
            make_task(task_type="A", duration_hours=2.0),
            make_task(task_type="A", duration_hours=6.0),
        ]
        model = StatsDurationModel.from_tasks(tasks)
        assert model.predict("Unknown") == 4.0  # global median(2, 6)

    def test_empty_tasks_returns_default(self):
        model = StatsDurationModel.from_tasks([])
        assert model.predict("any") == DEFAULT_DURATION_HOURS

    def test_skips_zero_and_none_durations(self):
        tasks = [
            make_task(task_type="A", duration_hours=0.0),
            make_task(task_type="A", duration_hours=3.0),
        ]
        model = StatsDurationModel.from_tasks(tasks)
        assert model.predict("A") == 3.0


# ── 2-opt Route Optimizer ─────────────────────────────────────────────────

def _make_routing_svc(dist_map: dict) -> MagicMock:
    """Mock routing service that returns distance from dist_map[(uwi_a, uwi_b)]."""
    svc = MagicMock()

    def route_between_points_or_none(lon_a, lat_a, lon_b, lat_b):
        # Identify well by lon (used as unique key in tests)
        key = (round(lon_a, 2), round(lon_b, 2))
        d = dist_map.get(key, dist_map.get((round(lon_b, 2), round(lon_a, 2)), 999.0))
        return {"distance_km": d}

    svc.route_between_points_or_none.side_effect = route_between_points_or_none
    return svc


class TestTwoOpt:
    def _make_wells_map(self, lons: dict) -> dict:
        """lons: {uwi: lon}. lat=0 for all."""
        wells = {}
        for uwi, lon in lons.items():
            w = MagicMock()
            w.lon = lon
            w.lat = 0.0
            wells[uwi] = w
        return wells

    def test_short_list_returned_as_is(self):
        tasks = [make_task("t1", destination_uwi="W1")]
        result = two_opt_order(tasks, {}, MagicMock())
        assert result == tasks

    def test_two_tasks_returned_as_is(self):
        tasks = [make_task("t1", destination_uwi="W1"), make_task("t2", destination_uwi="W2")]
        result = two_opt_order(tasks, {}, MagicMock())
        assert len(result) == 2

    def test_improves_bad_order(self):
        """
        4 wells at lons 1, 2, 3, 4.
        Bad order: W1→W3→W2→W4 (distance 2+1+2=5)
        Good order: W1→W2→W3→W4 (distance 1+1+1=3)
        2-opt should find the better ordering.
        """
        tasks = [
            make_task("t1", destination_uwi="W1"),
            make_task("t3", destination_uwi="W3"),
            make_task("t2", destination_uwi="W2"),
            make_task("t4", destination_uwi="W4"),
        ]
        wells_map = self._make_wells_map({"W1": 1.0, "W2": 2.0, "W3": 3.0, "W4": 4.0})

        dist_map = {
            (1.0, 2.0): 1.0, (1.0, 3.0): 2.0, (1.0, 4.0): 3.0,
            (2.0, 3.0): 1.0, (2.0, 4.0): 2.0,
            (3.0, 4.0): 1.0,
        }
        routing_svc = _make_routing_svc(dist_map)

        result = two_opt_order(tasks, wells_map, routing_svc)
        uwis = [t.destination_uwi for t in result]
        # Result should be sequential order (1→2→3→4 or reversed 4→3→2→1)
        assert uwis in (["W1", "W2", "W3", "W4"], ["W4", "W3", "W2", "W1"])
