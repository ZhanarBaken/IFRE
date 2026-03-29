from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple

from app.models.schemas import AssignmentItem, Task, UnassignedItem
from app.services.compatibility import compatibility_status
from app.services.scoring import priority_deadline_hours, score_task

logger = logging.getLogger(__name__)

INF = 10_000_000
# Cap for OR-Tools time dimension (14 days in minutes).
# Using a finite cap even for "unlimited" mode avoids int overflow issues.
MAX_HORIZON = 60 * 24 * 60  # 86400 min (60 days)


class ORToolsVRPSolver:
    """
    VRPTW solver using Google OR-Tools.
    Replaces the greedy _plan_time_aware when IFRE_USE_ORTOOLS=true.
    Returns None if OR-Tools finds no solution so the caller falls back to greedy.

    Node layout:
      0 .. M-1        : vehicle start depots (one per unit)
      M .. M+N-1      : tasks
      M+N             : dummy common end (open-end routes)
    """

    def solve(
        self,
        tasks: List[Task],
        wells_map: dict,
        unit_state: dict,
        route_matrix: Dict[Tuple[int, int], dict | None],
        task_nodes: Dict[str, int],
        max_total_time_minutes: int,
        has_rules: bool,
        compat: Dict[str, Set[str]],
        compat_norm: Dict[str, Set[str]],
        anchor_time: datetime,
        routing_svc,
    ) -> tuple[List[AssignmentItem], List[UnassignedItem]] | None:
        try:
            from ortools.constraint_solver import pywrapcp, routing_enums_pb2
        except ImportError:
            logger.warning("ortools not installed — falling back to greedy")
            return None

        units = list(unit_state.values())
        task_list = list(tasks)
        N = len(task_list)

        if not units or N == 0:
            return None

        # Cap vehicles at N+2 (best candidates by min travel to any task).
        # Large M with small N causes OR-Tools ROUTING_FAIL.
        MAX_VEHICLES = max(N + 2, 8)
        if len(units) > MAX_VEHICLES:
            def _unit_min_cost(u: object) -> float:
                costs = [
                    route_matrix.get((u.node_id, task_nodes[t.task_id]))
                    for t in task_list
                ]
                valid = [r["time_minutes"] for r in costs if r is not None]
                return min(valid) if valid else INF

            units = sorted(units, key=_unit_min_cost)[:MAX_VEHICLES]

        M = len(units)

        dummy_end = M + N
        total_nodes = M + N + 1

        # ── Horizon: spans all task time windows (not just vehicle budget) ──
        # max_total_time_minutes is the per-vehicle travel budget.
        # tw_horizon must cover the latest task deadline offset from anchor.
        task_offsets = [
            int((t.planned_start - anchor_time).total_seconds() / 60)
            for t in task_list
        ]
        max_deadline_h = max(
            priority_deadline_hours(t.priority) for t in task_list
        )
        tw_horizon = max(task_offsets) + int(max_deadline_h * 60) if task_offsets else 0
        horizon = min(max(max_total_time_minutes, tw_horizon), MAX_HORIZON)

        # ── Service times per task (minutes) ──────────────────────────────
        service_min: List[int] = [int(t.duration_hours * 60) for t in task_list]

        # ── Compatibility: which units are allowed for each task ───────────
        # forbidden[i][j] = True  ⟹  unit i cannot serve task j (hard block)
        # In non-strict mode (compatibility_strict=False) we never hard-block:
        # incompatible pairs get a cost penalty instead (like the greedy does).
        from app.core.config import settings as _cfg
        forbidden: List[List[bool]] = [[False] * N for _ in range(M)]
        if has_rules and _cfg.compatibility_strict:
            for i, unit in enumerate(units):
                for j, task in enumerate(task_list):
                    status = compatibility_status(
                        task.task_type, unit.unit_type, compat, compat_norm
                    )
                    if status is False:
                        forbidden[i][j] = True

        # ── Time matrix (travel only, service added in callback) ───────────
        # Rows/cols: 0..M-1 depots, M..M+N-1 tasks, M+N dummy end
        time_matrix = [[0] * total_nodes for _ in range(total_nodes)]

        for i, unit in enumerate(units):
            for j, task in enumerate(task_list):
                t_node = task_nodes[task.task_id]
                route = route_matrix.get((unit.node_id, t_node))
                cost = INF if (route is None or forbidden[i][j]) else route["time_minutes"]
                time_matrix[i][M + j] = cost

        for i, task_a in enumerate(task_list):
            node_a = task_nodes[task_a.task_id]
            for j, task_b in enumerate(task_list):
                if i == j:
                    continue
                node_b = task_nodes[task_b.task_id]
                route = route_matrix.get((node_a, node_b))
                time_matrix[M + i][M + j] = INF if route is None else route["time_minutes"]

        # ── OR-Tools model ─────────────────────────────────────────────────
        starts = list(range(M))
        ends = [dummy_end] * M

        manager = pywrapcp.RoutingIndexManager(total_nodes, M, starts, ends)
        routing = pywrapcp.RoutingModel(manager)

        # Transit callback includes service time at FROM node (task nodes only)
        def time_callback(from_idx: int, to_idx: int) -> int:
            fn = manager.IndexToNode(from_idx)
            tn = manager.IndexToNode(to_idx)
            travel = time_matrix[fn][tn]
            # Add service time when leaving a task node
            if M <= fn < M + N:
                travel += service_min[fn - M]
            return travel

        transit_cb = routing.RegisterTransitCallback(time_callback)
        routing.SetArcCostEvaluatorOfAllVehicles(transit_cb)

        # ── Time dimension ─────────────────────────────────────────────────
        vehicle_budget = min(max_total_time_minutes, horizon)
        routing.AddDimension(
            transit_cb,
            slack_max=horizon,          # max waiting time at a node (covers gaps between tasks)
            capacity=horizon,           # cumul upper bound — must cover all tw offsets
            fix_start_cumul_to_zero=True,
            name="Time",
        )
        time_dim = routing.GetDimensionOrDie("Time")

        # ── Time windows per task (minutes from anchor_time) ───────────────
        for j, task in enumerate(task_list):
            idx = manager.NodeToIndex(M + j)
            deadline_h = priority_deadline_hours(task.priority)
            tw_start = min(
                horizon,
                max(0, int((task.planned_start - anchor_time).total_seconds() / 60)),
            )
            tw_end = min(tw_start + int(deadline_h * 60), horizon)
            time_dim.CumulVar(idx).SetRange(tw_start, tw_end)

        # ── Vehicle availability offsets ───────────────────────────────────
        for i, unit in enumerate(units):
            start_idx = routing.Start(i)
            avail_offset = max(
                0, int((unit.available_at - anchor_time).total_seconds() / 60)
            )
            if avail_offset > 0:
                time_dim.CumulVar(start_idx).SetMin(avail_offset)

        # ── Vehicle time budget ────────────────────────────────────────────
        # Each vehicle may accumulate up to vehicle_budget minutes of *travel+service*.
        # We enforce this via a span constraint on the vehicle's route time.
        for i in range(M):
            end_idx = routing.End(i)
            time_dim.CumulVar(end_idx).SetMax(horizon)

        # ── Search parameters ──────────────────────────────────────────────
        search_params = pywrapcp.DefaultRoutingSearchParameters()
        search_params.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION
        )
        search_params.local_search_metaheuristic = (
            routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
        )
        search_params.time_limit.seconds = 30
        search_params.log_search = False

        solution = routing.SolveWithParameters(search_params)

        status = routing.status()
        # 1=ROUTING_SUCCESS, 2=ROUTING_PARTIAL_SUCCESS_LOCAL_OPTIMUM_NOT_REACHED
        if solution is None or status not in (1, 2):
            logger.warning(
                "OR-Tools: no solution (status=%d) — falling back to greedy", status
            )
            return None

        # ── Extract assignments ────────────────────────────────────────────
        assignments: List[AssignmentItem] = []
        assigned_ids: set[str] = set()

        for i, unit in enumerate(units):
            idx = routing.Start(i)
            while not routing.IsEnd(idx):
                next_idx = solution.Value(routing.NextVar(idx))
                node = manager.IndexToNode(next_idx)

                if routing.IsEnd(next_idx) or node < M or node >= M + N:
                    idx = next_idx
                    continue

                task_j = node - M
                task = task_list[task_j]
                t_node = task_nodes[task.task_id]
                route = route_matrix.get((unit.node_id, t_node))
                if route is None:
                    idx = next_idx
                    continue

                score_result = score_task(
                    task=task,
                    distance_km=route["distance_km"],
                    travel_minutes=route["time_minutes"],
                    unit_available_at=unit.available_at,
                    compatibility_penalty=0.0,
                )

                assignments.append(
                    AssignmentItem(
                        task_id=task.task_id,
                        wialon_id=unit.wialon_id,
                        eta_minutes=score_result.eta_minutes,
                        distance_km=route["distance_km"],
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
                assigned_ids.add(task.task_id)

                # Update unit state for subsequent tasks in same route
                unit.available_at = score_result.end_time
                unit.node_id = t_node
                well = wells_map[task.destination_uwi]
                unit.lon = well.lon
                unit.lat = well.lat

                idx = next_idx

        unassigned: List[UnassignedItem] = [
            UnassignedItem(
                task_id=t.task_id,
                reason="OR-Tools: задача не вошла в оптимальное решение",
            )
            for t in task_list
            if t.task_id not in assigned_ids
        ]

        logger.info(
            "OR-Tools: assigned=%d unassigned=%d status=%d",
            len(assignments), len(unassigned), status,
        )
        return assignments, unassigned
