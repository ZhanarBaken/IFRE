# IFRE routing service (mock-first)

This service runs on mock data that mirrors the structure of the real entities. The architecture is designed to connect to a production PostgreSQL database without changing business logic.

## TL;DR

1. Run locally: `make up`
2. Open docs: `http://127.0.0.1:8000/docs`
3. Try a demo scenario (see below)

## Run with Docker (local)

1. Fill local env file: `.envs/.local/.env`
2. Start: `make up`

Local dev with auto-reload:

```bash
docker compose -f local.yml up
```

## Run with Docker (production)

1. Fill production env file: `.envs/.production/.env`
2. Start: `make up-prod`

## Run without Docker

1. Install deps: `pip install -r requirements.txt`
2. Set env vars in your shell, or create `.env` in project root
3. Run API: `uvicorn app.main:app --reload`

## Connect a real PostgreSQL DB locally

1. Put these into `.envs/.local/.env`:

```
IFRE_DATA_SOURCE=postgres
IFRE_DB_URL=postgresql+psycopg://user:pass@host:5432/dbname
```

2. Start as usual:

```
make up
```

If the DB is missing required tables or columns, the service will fail fast with a clear error.

## Env files and behavior

- Local compose uses `.envs/.local/.env`
- Production compose uses `.envs/.production/.env`
- The app reads OS environment variables. It does not read `.envs/*` directly.
- For non-docker runs, you can use a root `.env` file.

## Key env vars

- `IFRE_DATA_SOURCE`: `mock` or `postgres`
- `IFRE_DB_URL`: PostgreSQL URL (required for `postgres`)
- `IFRE_AVG_SPEED_KMPH`: average speed for ETA
- `IFRE_EDGE_WEIGHT_IN_METERS`: `1` (default) if road edges are in meters
- `IFRE_USE_TASK_ASSIGNMENTS`: `true/false` — whether to use the optional assignments table

## Endpoints

- `POST /api/recommendations`
- `POST /api/route`
- `POST /api/matrix`
- `POST /api/multitask`
- `POST /api/assignments`
- `GET /demo/route-map?wialon_id=1001&uwi=W-001`
- `GET /demo/batch-plan?start_date=2025-02-20&shift=day`

All request/response schemas are in `app/models/schemas.py`.

## Demo pages (open in browser)

- Single route map:
  - `http://127.0.0.1:8000/demo/route-map?wialon_id=1001&uwi=W-001`
- Batch plan map + tables:
  - `http://127.0.0.1:8000/demo/batch-plan?start_date=2025-02-20&shift=day`

If you use `curl`, you will only see raw HTML. For the map, open the URL in a browser.

## Demo scenarios (from the spec)

Scenario 1 — urgent task (high priority):

```bash
curl -X POST http://127.0.0.1:8000/api/recommendations \
  -H 'Content-Type: application/json' \
  -d '{
    "task_id": "T-2025-0042",
    "priority": "high",
    "destination_uwi": "W-001",
    "planned_start": "2025-02-20T08:00:00",
    "duration_hours": 4.5
  }'
```

Scenario 2 — baseline vs optimized:

```bash
curl -X POST http://127.0.0.1:8000/api/recommendations \
  -H 'Content-Type: application/json' \
  -d '{
    "task_id": "T-2025-0043",
    "priority": "medium",
    "destination_uwi": "W-002",
    "planned_start": "2025-02-20T09:00:00",
    "duration_hours": 3.0,
    "mode": "baseline"
  }'
```

Then run the same request without `mode` to see optimized ranking.

Exclude busy units (optional):

```bash
curl -X POST http://127.0.0.1:8000/api/recommendations \
  -H 'Content-Type: application/json' \
  -d '{
    "task_id": "T-2025-0043",
    "priority": "medium",
    "destination_uwi": "W-002",
    "planned_start": "2025-02-20T09:00:00",
    "duration_hours": 3.0,
    "exclude_busy": true
  }'
```

Scenario 3 — multitask grouping:

```bash
curl -X POST http://127.0.0.1:8000/api/multitask \
  -H 'Content-Type: application/json' \
  -d '{
    "task_ids": ["T-2025-0042", "T-2025-0043", "T-2025-0044"],
    "constraints": { "max_total_time_minutes": 480, "max_detour_ratio": 1.3 }
  }'
```

## Scoring (short)

```
cost = w_distance * distance_km
     + w_eta * travel_minutes
     + w_wait * wait_minutes * priority_weight
     + w_late * late_minutes * priority_weight
score = 1 / (1 + cost)
```

Priority weights: `high=0.55`, `medium=0.35`, `low=0.10`.

## ETA / duration forecast (no LLM)

We intentionally do **not** use an LLM for ETA or duration. These are numeric forecasts and must be stable and reproducible.

Important:
- For ML training we need **historical actual durations** (fact start/end). If there is no history, ML cannot be trained.

Hackathon compromise:
- **Data‑driven baseline**: median duration by `task_type` from the tasks table (no training). If `duration_hours` is missing or `<= 0`, we substitute the median (fallback default `2.0h` if there is no history).
- **ML stub**: a placeholder module that returns `None` until real факты выполнения are available. Once you have history, replace it with a regression model using the same interface.

## Current plan (optional)

If your DB has a separate table for current assignments (e.g. `task_assignments`),
set `IFRE_USE_TASK_ASSIGNMENTS=true` and the service will treat those tasks as already assigned and mark
the corresponding vehicles as busy.

If this flag is `false`, the service does not query assignments at all (no errors if the table is absent).
