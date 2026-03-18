# IFRE Routing Service

Интеллектуальный сервис маршрутизации и назначения спецтехники по заявкам.
Работает поверх дорожного графа месторождения, учитывает ETA, приоритеты, окна смен,
совместимость типов работ и техники. Подходит для ручного подтверждения диспетчером.

## Возможности

- Маршруты строго по графу дорог (не по прямой)
- ETA и расстояние для каждой заявки
- Ранжирование техники по скорингу (distance/ETA/wait/late)
- Batch‑планирование на дату/смену с multi‑stop группировкой
- Визуальная витрина (карта + таблицы)
- Объяснения решений (reason)

## Быстрый старт

1. Создайте локальный env: `make env`
2. Заполните `.envs/.local/.env`
3. Запустите: `make up`
4. Откройте документацию: `http://127.0.0.1:8000/docs`

## Запуск в Docker (локально)

```
make up
```

Локальная разработка с авто‑перезапуском:

```
docker compose -f local.yml up
```

## Запуск в Docker (production)

1. Заполните `.envs/.production/.env`
2. Запустите: `make up-prod`

## Запуск без Docker

1. Установите зависимости: `pip install -r requirements.txt`
2. Задайте переменные окружения, например:

```
source .envs/.local/.env
```

3. Запустите API:

```
uvicorn app.main:app --reload
```

## Файлы окружения

- Локальный compose читает `.envs/.local/.env`
- Production compose читает `.envs/.production/.env`
- Приложение читает только переменные окружения
- `.env.example` — полный список всех переменных

## Ключевые переменные

- `IFRE_DB_URL`: строка подключения PostgreSQL
- `IFRE_DB_SCHEMA`: схема БД (обычно `references`)
- `IFRE_AVG_SPEED_KMPH`: средняя скорость для ETA
- `IFRE_MIN_SPEED_KMPH` / `IFRE_MAX_SPEED_KMPH`: фильтр мусорных скоростей
- `IFRE_EDGE_WEIGHT_IN_METERS`: `1`, если веса рёбер в метрах
- `IFRE_GRAPH_BIDIRECTIONAL`: `true/false` — форсировать двунаправленный граф
- `IFRE_GRAPH_BIDIRECTIONAL_THRESHOLD`: порог авто‑детекта
- `IFRE_TASK_DOCUMENT_CODES`: коды документов EAV для задач
- `IFRE_EAV_MAPPING_FILE`: JSON‑маппинг полей EAV
- `IFRE_COMPATIBILITY_STRICT`: строгая совместимость (несовместимые исключаются)
- `IFRE_COMPATIBILITY_PENALTY`: штраф за несовместимость в soft‑режиме
- `IFRE_USE_SNAPSHOT_BY_PLANNING_DATE`: брать снапшоты техники ближе к дате планирования
- `IFRE_ANCHOR_UNITS_AT_PLAN_START`: якорить доступность техники на старт планирования
- `IFRE_ASSIGNMENTS_GROUPING`: включить multi‑stop группировку в batch‑планировании

## Данные и соответствие ТЗ

Используются данные БД:

- `references.road_nodes`, `references.road_edges` — граф дорог
- `references.wells` — точки назначения (uwi, lon/lat)
- `references.wialon_units_snapshot_*` — позиции техники
- `tasks` (если есть) или EAV‑схема `dcm.records` + `dcm.record_indicator_values`

Если таблицы `tasks` нет, сервис собирает заявки из EAV по `IFRE_TASK_DOCUMENT_CODES`.
Записи без валидной скважины исключаются (см. `/api/tasks/debug`).

## Совместимость техники (4.6)

Словарь совместимости строится из EAV:

- пары `WKIND` (тип работ) ↔ `VEHKIND` (тип техники)
- тип техники подтягивается по техкарточкам (`TRS_VEHCARD_SNUM` → `TRS_VEHCARD_CLASS`)

Режимы:

- `IFRE_COMPATIBILITY_STRICT=true` — несовместимые или `unknown` исключаются
- `IFRE_COMPATIBILITY_STRICT=false` — несовместимые допускаются со штрафом

## Скоринг

Скоринг минимизирует стоимость:

- расстояние
- ETA
- ожидание до planned_start
- опоздание относительно SLA

Весами управляют `IFRE_SCORE_W_*`.

## Эндпоинты

- `POST /api/recommendations`
- `POST /api/route`
- `POST /api/matrix`
- `POST /api/multitask`
- `POST /api/assignments`
- `GET /api/tasks`
- `GET /api/tasks/debug`
- `GET /demo/route-map?wialon_id=...&uwi=...`
- `GET /demo/batch-plan?start_date=YYYY-MM-DD&shift=day|night`

Схемы запросов/ответов — в `app/models/schemas.py`.

`POST /api/assignments` принимает флаг `grouping` (`true/false`).
Если не задан — используется `IFRE_ASSIGNMENTS_GROUPING`.

## Демо‑страницы

- Маршрут одной заявки:

```
http://127.0.0.1:8000/demo/route-map?wialon_id=1001&uwi=W-001
```

- Batch‑планирование (карта + таблицы):

```
http://127.0.0.1:8000/demo/batch-plan?start_date=2025-07-31&shift=night
```

Группировка включена по умолчанию. Чтобы отключить:

```
...&grouping=false
```

## Примеры запросов (curl)

### 1) Рекомендации

```
curl -X POST http://127.0.0.1:8000/api/recommendations \
  -H 'Content-Type: application/json' \
  -d '{
    "task_id": "12649",
    "priority": "low",
    "destination_uwi": "AIR_0003",
    "planned_start": "2025-10-29T08:00:00",
    "duration_hours": 1
  }'
```

### 2) Маршрут по графу

```
curl -X POST http://127.0.0.1:8000/api/route \
  -H 'Content-Type: application/json' \
  -d '{"from":{"wialon_id":29935360},"to":{"uwi":"AIR_0003"}}'
```

### 3) Matrix

```
curl -X POST http://127.0.0.1:8000/api/matrix \
  -H 'Content-Type: application/json' \
  -d '{"start_nodes":[3840],"end_nodes":[2977]}'
```

### 4) Multitask

```
curl -X POST http://127.0.0.1:8000/api/multitask \
  -H 'Content-Type: application/json' \
  -d '{
    "task_ids":["12649","12653","12686"],
    "constraints":{"max_total_time_minutes":480,"max_detour_ratio":1.3}
  }'
```

### 5) Batch‑планирование

```
curl -X POST http://127.0.0.1:8000/api/assignments \
  -H 'Content-Type: application/json' \
  -d '{"filters":{"start_date":"2025-10-29","end_date":"2025-11-05"},"grouping":true}'
```

## Диагностика

- `GET /api/tasks/debug` — почему часть записей не попала в задачи
- Ошибки БД или отсутствующие таблицы приводят к явным исключениям
- Для тяжёлых сценариев уменьшайте диапазон дат и `limit`
