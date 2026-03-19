# IFRE Routing Service

Прототип сервиса маршрутизации и назначения спецтехники на месторождении.
Строит маршруты по дорожному графу, рассчитывает ETA и скоринг, поддерживает группировку заявок (multi‑stop) и демонстрацию на карте.

## TL;DR

1. `make env`
2. заполнить `.envs/.local/.env`
3. `make up`
4. `http://127.0.0.1:8000/docs` и демо‑страницы

## Возможности

- Маршрутизация строго по графу дорог (не по прямой)
- ETA и расстояние для каждой заявки
- Ранжирование техники по скорингу (distance/ETA/wait/late/SLA)
- Batch‑планирование на дату/смену с multi‑stop группировкой
- Визуальная витрина (карта + таблицы)
- Понятные причины назначений и отказов
- Опциональная AI‑переформулировка `reason`

## Архитектура

```
API (FastAPI)
  ├─ Recommendations → Scoring
  ├─ Routing / Matrix
  ├─ Multitask (grouping)
  └─ Assignments (batch plan)
        ↓
RoutingService → Graph (road_nodes/road_edges)
Repository → PostgreSQL (references + tasks/EAV)
```

## Быстрый старт

```
make env
make up
```

Документация: `http://127.0.0.1:8000/docs`

## Переменные окружения

- `.envs/.local/.env` — локальный запуск
- `.envs/.production/.env` — production
- `.env.example` — полный список

Ключевые:

- `IFRE_DB_URL` — PostgreSQL
- `IFRE_DB_SCHEMA` — схема (обычно `references`)
- `IFRE_AVG_SPEED_KMPH`, `IFRE_MIN_SPEED_KMPH`, `IFRE_MAX_SPEED_KMPH`
- `IFRE_EDGE_WEIGHT_IN_METERS` — веса рёбер в метрах
- `IFRE_SCORE_W_DISTANCE`, `IFRE_SCORE_W_ETA`, `IFRE_SCORE_W_WAIT`, `IFRE_SCORE_W_LATE`
- `IFRE_SCORE_POINTS_SCALE` — шкала перевода cost → score 0–100
- `IFRE_COMPATIBILITY_STRICT` / `IFRE_COMPATIBILITY_PENALTY`
- `IFRE_USE_SNAPSHOT_BY_PLANNING_DATE`
- `IFRE_ANCHOR_UNITS_AT_PLAN_START`
- `IFRE_ASSIGNMENTS_GROUPING`
- `IFRE_MAX_TOTAL_TIME_MINUTES_DEFAULT`

### AI‑reason (опционально)

```
IFRE_REASON_AI_ENABLED=true
IFRE_REASON_AI_API_URL=https://llm.alem.ai/v1/chat/completions
IFRE_REASON_AI_MODEL=qwen3
IFRE_REASON_AI_API_KEY=... 
```

## Данные и соответствие ТЗ

Используются данные БД:

- `references.road_nodes`, `references.road_edges` — граф дорог
- `references.wells` — скважины (uwi, lon/lat)
- `references.wialon_units_snapshot_*` — позиции техники
- `tasks` (если есть) или EAV `dcm.records` + `dcm.record_indicator_values`

Если таблицы `tasks` нет, заявки собираются из EAV по `IFRE_TASK_DOCUMENT_CODES`.
Записи без валидной скважины исключаются (см. `/api/tasks/debug`).

## Скоринг

Внутренняя стоимость:

```
cost = Wd*distance_km + We*eta_min + Ww*wait_min*priority_weight + Wl*late_min*priority_weight
score = 100 / (1 + cost/IFRE_SCORE_POINTS_SCALE)
```

SLA‑дедлайны по приоритету:
- high: +2 часа
- medium: +5 часов
- low: +12 часов

## Совместимость

Если есть словарь совместимости:
- `IFRE_COMPATIBILITY_STRICT=true` — несовместимые исключаются
- `IFRE_COMPATIBILITY_STRICT=false` — допускаются со штрафом `IFRE_COMPATIBILITY_PENALTY`

## Эндпоинты

- `POST /api/recommendations`
- `POST /api/route`
- `POST /api/matrix`
- `POST /api/multitask`
- `POST /api/assignments`
- `GET /api/tasks`
- `GET /api/tasks/debug`
- `GET /demo/route-map`
- `GET /demo/batch-plan`

## Демонстрация: 3 сценария

### Сценарий 1 — срочная заявка (high)

Запрос:

```bash
curl -X POST 'http://127.0.0.1:8000/api/recommendations?top_units=3' \
  -H 'Content-Type: application/json' \
  -d '{
    "task_id":"10067",
    "priority":"high",
    "destination_uwi":"JET_4055",
    "planned_start":"2025-07-31T20:00:00",
    "duration_hours":11.0,
    "task_type":"Опрессовка кондуктора Ø 245 мм"
  }'
```

Маршрут (топ‑1 техника):

```
http://127.0.0.1:8000/demo/route-map?wialon_id=29935360&uwi=JET_4055
```

Ожидаемо: система выбирает технику и даёт короткое объяснение (ETA, SLA, совместимость).

### Сценарий 2 — сравнение baseline vs optimized

Чтобы увидеть различия, включите более реалистичную доступность техники:

```
IFRE_ANCHOR_UNITS_AT_PLAN_START=false
```

Два запроса на одну заявку:

```bash
# optimized
curl -X POST 'http://127.0.0.1:8000/api/recommendations?top_units=1' \
  -H 'Content-Type: application/json' \
  -d '{
    "task_id":"12635",
    "priority":"medium",
    "destination_uwi":"JET_4416",
    "planned_start":"2025-09-07T20:00:00",
    "duration_hours":2.0,
    "task_type":"СК3-2-5 Телеметрия"
  }'

# baseline
curl -X POST 'http://127.0.0.1:8000/api/recommendations?top_units=1' \
  -H 'Content-Type: application/json' \
  -d '{
    "task_id":"12635",
    "priority":"medium",
    "destination_uwi":"JET_4416",
    "planned_start":"2025-09-07T20:00:00",
    "duration_hours":2.0,
    "task_type":"СК3-2-5 Телеметрия",
    "mode":"baseline"
  }'
```

Визуализация двух маршрутов:

```
http://127.0.0.1:8000/demo/route-map?wialon_id=26455455&uwi=JET_4416
http://127.0.0.1:8000/demo/route-map?wialon_id=29935360&uwi=JET_4416
```

Ожидаемо: baseline выбирает ближайшую по расстоянию, optimized учитывает доступность и SLA.

### Сценарий 3 — многозадачность (3 заявки рядом)

Сравниваем группировку и раздельное обслуживание:

```bash
curl -X POST http://127.0.0.1:8000/api/multitask \
  -H 'Content-Type: application/json' \
  -d '{
    "task_ids":["12649","12653","12686"],
    "constraints":{"max_total_time_minutes":20000,"max_detour_ratio":1.3}
  }'
```

Витрина с группировкой:

```
http://127.0.0.1:8000/demo/batch-plan?task_ids=12649,12653,12686&grouping=true&max_total_time_minutes=20000&max_detour_ratio=1.3
```

Витрина без группировки:

```
http://127.0.0.1:8000/demo/batch-plan?task_ids=12649,12653,12686&grouping=false&max_total_time_minutes=20000&max_detour_ratio=1.3
```

Ожидаемо: при `grouping=true` задачи объединяются в один выезд (одна техника), экономия показывается в `reason`.

## Диагностика

- `GET /api/tasks/debug` — почему часть записей не попала в задачи
- Ошибки отсутствующих таблиц не скрываются

## Ограничения и планы

- Качество графа и снапшотов напрямую влияет на маршрут и SLA
- При наличии фактической истории можно добавить ML‑прогноз ETA/длительности
