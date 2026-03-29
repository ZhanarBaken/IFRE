from __future__ import annotations

import logging
from typing import List

from app.models.schemas import Task

logger = logging.getLogger(__name__)


def two_opt_order(
    tasks: List[Task],
    wells_map: dict,
    routing_svc,
) -> List[Task]:
    """
    Improve task ordering within a single route using 2-opt local search.

    Starting from an initial permutation (typically nearest-neighbor),
    tries all O(n²) segment reversals per pass and accepts the first
    improvement found (first-improvement strategy).  Repeats until no
    improving swap exists.

    Complexity: O(n² · passes · routing_calls).
    For small groups (n ≤ 5, typical in field ops) this is negligible.
    """
    if len(tasks) <= 2:
        return list(tasks)

    def route_dist(order: List[Task]) -> float:
        total = 0.0
        for a, b in zip(order, order[1:]):
            wa = wells_map[a.destination_uwi]
            wb = wells_map[b.destination_uwi]
            r = routing_svc.route_between_points_or_none(
                wa.lon, wa.lat, wb.lon, wb.lat
            )
            if r is None:
                return float("inf")
            total += r["distance_km"]
        return total

    best = list(tasks)
    best_dist = route_dist(best)
    improved = True

    while improved:
        improved = False
        for i in range(len(best) - 1):
            for j in range(i + 2, len(best)):
                # Reverse segment [i+1 .. j] and measure new total distance
                candidate = best[: i + 1] + best[i + 1 : j + 1][::-1] + best[j + 1 :]
                d = route_dist(candidate)
                if d < best_dist - 1e-9:
                    best = candidate
                    best_dist = d
                    improved = True
                    break
            if improved:
                break

    if best != list(tasks):
        logger.debug("two_opt_order: improved %.2f → %.2f km", route_dist(list(tasks)), best_dist)

    return best
