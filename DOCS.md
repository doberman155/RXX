# Документация проекта: интернет-магазин на событийных микросервисах

## 1. Обзор

Монорепозиторий на **Go 1.26** (модуль `github.com/hshsb/shop`), реализующий бэкенд интернет-магазина из четырёх независимых микросервисов. Сервисы общаются между собой **только через события Kafka** (Redpanda) — синхронных вызовов между ними нет.

Ключевые принципы:

- **Database per service** — у каждого сервиса своя база PostgreSQL.
- **Transactional outbox** — события сначала пишутся в таблицу `outbox` в той же транзакции, что и бизнес-данные, а фоновый релеер публикует их в Kafka (at-least-once).
- **Сага резервирования** — оформление заказа идёт через асинхронный резерв товара с компенсацией при отказе.
- **Идемпотентность** — повторное создание заказа по `Idempotency-Key` возвращает тот же заказ; консьюмеры защищены таблицей `processed_events`.
- **DLQ** — после исчерпания ретраев сообщения уходят в топик `<topic>.dlq`.
- **JWT-аутентификация** с ролями `USER` и `ADMIN`.
- **Graceful shutdown** — по SIGTERM все компоненты останавливаются в пределах `SHUTDOWN_TIMEOUT`.

## 2. Архитектура

```
                        ┌─────────────┐
  Клиент (REST) ──────► │ user-service │ :8081   (JWT, регистрация)
                        └─────────────┘
                        ┌────────────────┐        order.created ──────────┐
  Клиент (REST) ──────► │ order-service  │ :8083 ─┐                       │
                        └────────────────┘        │                       ▼
                                  ▲               ▼              ┌─────────────────┐
                                  │            ┌──────────────────────────┐        │
                                  │            │  Kafka (Redpanda)        │        │
                                  │            └──────────────────────────┘        │
                                  │               │                       ▲        │
                   stock.reserved │               ▼                       │        │
                   stock.rejected │        ┌─────────────────┐  order.created      │
                                  └─────── │ product-service │ :8082                │
                                           └─────────────────┘                      │
                        ┌──────────────────────┐        order.created /             │
  Клиент (REST) ──────► │ notification-service │ :8084 ◄── order.status-changed ─────┘
                        └──────────────────────┘
```

| Сервис | Порт | БД | Назначение |
|---|---|---|---|
| `user-service` | 8081 | `shop_user` | Регистрация, вход, выпуск JWT, роли |
| `product-service` | 8082 | `shop_product` | Каталог, остатки, резерв/возврат по событиям |
| `order-service` | 8083 | `shop_order` | Заказы, статусы, оркестрация саги |
| `notification-service` | 8084 | `shop_notification` | Уведомления о смене статуса заказа |
| `kafka-init` (cmd/kafka-admin) | — | — | Одноразовое создание топиков |

Инфраструктура: **Redpanda** v25.1.1 (Kafka-совместимый брокер без ZooKeeper; с хоста доступен на `localhost:19092`), **PostgreSQL 17** на хостовом порту **5433**.

## 3. Сервисы и REST-эндпоинты

Все сервисы отдают `GET /healthz` и `GET /readyz`. Ошибки возвращаются в едином формате:

```json
{"error": {"code": "...", "message": "...", "fields": {}, "request_id": "..."}}
```

### user-service (порт 8081)

| Метод и путь | Доступ | Описание |
|---|---|---|
| `POST /api/v1/auth/register` | все | Регистрация (роль всегда `USER`) |
| `POST /api/v1/auth/login` | все | Вход, возвращает JWT |
| `GET /api/v1/users/me` | JWT | Данные текущего пользователя |
| `GET /api/v1/users?limit&offset` | ADMIN | Список пользователей |
| `GET /api/v1/users/{id}` | ADMIN | Пользователь по ID |

Первый администратор создаётся при старте из `BOOTSTRAP_ADMIN_EMAIL` / `BOOTSTRAP_ADMIN_PASSWORD`.

### product-service (порт 8082)

| Метод и путь | Доступ | Описание |
|---|---|---|
| `GET /api/v1/products?category&limit&offset` | все | Список товаров |
| `GET /api/v1/products/{id}` | все | Товар по ID |
| `POST /api/v1/products` | ADMIN | Создать товар |
| `PUT /api/v1/products/{id}` | ADMIN | Обновить товар |
| `DELETE /api/v1/products/{id}` | ADMIN | Мягкое удаление (`deleted_at`) |

Цены хранятся в копейках (`price_cents`), валюта — RUB. Резервирования через REST нет — только по событиям Kafka.

### order-service (порт 8083)

| Метод и путь | Доступ | Описание |
|---|---|---|
| `POST /api/v1/orders` | USER/ADMIN | Создать заказ (обязателен заголовок `Idempotency-Key`) |
| `GET /api/v1/orders?status&limit&offset` | свои / ADMIN — все | Список заказов |
| `GET /api/v1/orders/{id}` | свой / ADMIN | Заказ по ID (чужой → 404) |
| `POST /api/v1/orders/{id}/complete` | владелец/ADMIN | RESERVED → COMPLETED |
| `POST /api/v1/orders/{id}/cancel` | владелец/ADMIN | Отмена из RESERVED (публикует компенсацию) |

Повтор `POST /orders` с тем же `Idempotency-Key` и тем же составом → `200` с тем же заказом; с другим составом → `409`.

### notification-service (порт 8084)

| Метод и путь | Доступ | Описание |
|---|---|---|
| `GET /api/v1/notifications?order_id&user_id&limit&offset` | свои / ADMIN — все | Уведомления |

## 4. События Kafka

Контракт событий — `pkg/events/events.go` (Schema Registry не используется). Ключ сообщения во всех топиках — `orderId`, поэтому все события одного заказа попадают в одну партицию и обрабатываются по порядку.

Общий конверт:

```json
{
  "event_id": "uuid",
  "event_type": "order.created",
  "occurred_at": "2024-01-01T00:00:00Z",
  "order_id": "uuid"
}
```

| Топик | Producer → Consumer | Нагрузка |
|---|---|---|
| `order.created` | order → product, notification | `user_id`, `items[]` (`product_id`, `quantity`) |
| `stock.reserved` | product → order | `items[]` с `name`/`unit_price_cents`, `total_amount_cents`, `currency` |
| `stock.rejected` | product → order | `reason` (`INSUFFICIENT_STOCK` / `PRODUCT_NOT_FOUND`), `message`, `items[]` (`requested`/`available`) |
| `order.cancelled` | order → product | `user_id`, `previous_status`, `reason`, `stock_was_reserved` — компенсация, product возвращает остаток |
| `order.status-changed` | order → notification | `user_id`, `old_status`, `new_status`, `total_amount_cents`, `reason` |

Цены едут внутри события `stock.reserved`, поэтому order-service не обращается к чужой базе. Для каждого топика существует DLQ-топик с суффиксом `.dlq`.

### Сага заказа

```
POST /orders ──► NEW ──► order.created ──► product резервирует ──► stock.reserved ──► RESERVED
                                                                        │
                                                              нехватка: stock.rejected ──► CANCELLED
RESERVED ──► COMPLETED   (вручную через /complete или автоматически при ORDER_AUTO_COMPLETE=true)
RESERVED ──► CANCELLED   (через /cancel: order.cancelled → product возвращает остаток)
```

Допустимые переходы: `NEW → RESERVED/CANCELLED`, `RESERVED → COMPLETED/CANCELLED`. Остальные → `409`.

## 5. Базы данных и миграции

Миграции — **goose**, встроены в бинарники через `embed` (`migrations/migrations.go`), применяются при старте под advisory-lock (можно отключить: `POSTGRES_MIGRATE_ON_START=false`).

| БД | Таблицы |
|---|---|
| `shop_user` | `users` (id, email unique lower, password_hash bcrypt, role CHECK USER/ADMIN) |
| `shop_product` | `products` (sku unique среди живых, price_cents, stock, deleted_at), `reservations` (status HELD/RELEASED, unique (order_id, product_id) — защита от двойного резерва), `outbox`, `processed_events` |
| `shop_order` | `orders` (status CHECK NEW/RESERVED/COMPLETED/CANCELLED), `order_items`, `idempotency_keys` (PK (user_id, key) + request_hash), `outbox`, `processed_events` |
| `shop_notification` | `notifications` (unique event_id), `processed_events` |

## 6. Запуск

### Всё сразу (Docker Compose)

```bash
docker compose up -d --build    # postgres, redpanda, kafka-init, 4 сервиса
curl localhost:8081/healthz     # проверка
docker compose down -v          # остановка с удалением данных
docker compose run --rm kafka-init   # пересоздать топики
```

> ⚠️ **Внимание:** `docker-compose.yaml` монтирует скрипт `./deploy/postgres/init-databases.sh`, но каталог `deploy/` в репозитории отсутствует — без этого файла первый старт PostgreSQL упадёт. Создайте скрипт создания баз `shop_user`, `shop_product`, `shop_order`, `shop_notification` или уберите volume из compose.

### Один Dockerfile на все сервисы

```bash
docker build --build-arg SERVICE=order-service -t shop/order-service .
```

Статический бинарник, alpine, запуск от non-root пользователя.

### Локальная разработка

```bash
go build ./...
go test ./...                        # юнит-тесты
go test -tags=integration ./...      # интеграционные (нужны PostgreSQL и Kafka;
                                     # переопределяются TEST_POSTGRES_DSN, TEST_KAFKA_BROKERS)
```

Покрытие интеграционными тестами ~76.5%.

### Демо-сценарий

`api/shop.http` — готовый сценарий для JetBrains HTTP Client / VS Code REST Client: вход админа → создание товаров → регистрация пользователя → заказ (NEW → RESERVED → COMPLETED) → повтор с тем же `Idempotency-Key` → заказ с нехваткой товара (CANCELLED) → просмотр уведомлений.

## 7. Конфигурация

Все переменные читаются из окружения (см. `.env.example`).

**Общие:**

| Переменная | По умолчанию | Описание |
|---|---|---|
| `HTTP_PORT` | — | Порт HTTP-сервера |
| `LOG_LEVEL` | `info` | Уровень логирования |
| `SHUTDOWN_TIMEOUT` | `20s` | Таймаут graceful shutdown |
| `POSTGRES_DSN` | — (обязательна) | Строка подключения к PostgreSQL |
| `POSTGRES_MAX_CONNS` | `10` | Размер пула соединений |
| `POSTGRES_MIGRATE_ON_START` | `true` | Применять миграции при старте |
| `KAFKA_BROKERS` | — | Список брокеров через запятую |
| `KAFKA_GROUP_ID` | — | Consumer group |
| `KAFKA_MAX_RETRIES` | `3` | Ретраи до отправки в DLQ |
| `KAFKA_RETRY_BACKOFF` | `500ms` | Начальный backoff |
| `KAFKA_MAX_RETRY_BACKOFF` | `10s` | Максимальный backoff |
| `KAFKA_DLQ_SUFFIX` | `.dlq` | Суффикс DLQ-топиков |
| `KAFKA_PARTITIONS` | `3` | Партиций на топик (у DLQ — 1) |
| `JWT_SECRET` | — (обязателен, ≥16 симв.) | Секрет подписи JWT |
| `JWT_TTL` | `24h` | Время жизни токена |
| `JWT_ISSUER` | `user-service` | Issuer токена |
| `OUTBOX_POLL_INTERVAL` | `500ms` | Интервал опроса outbox |
| `OUTBOX_BATCH_SIZE` | `100` | Размер батча релеера |

**Специфичные:**

| Переменная | Сервис | Описание |
|---|---|---|
| `ORDER_AUTO_COMPLETE` | order | Автоперевод RESERVED → COMPLETED (по умолчанию `true`) |
| `BCRYPT_COST` | user | Стоимость bcrypt (по умолчанию `10`) |
| `BOOTSTRAP_ADMIN_EMAIL` / `BOOTSTRAP_ADMIN_PASSWORD` | user | Первый админ, создаётся при старте |

В `.env.example`: пользователь БД `shop` / `shop_local_password`, админ `admin@shop.local` / `admin_local_password`.

## 8. Структура репозитория

```
cmd/                    Точки входа (тонкие обёртки над internal/<domain>.Run)
  user-service/         :8081
  product-service/      :8082
  order-service/        :8083
  notification-service/ :8084
  kafka-admin/          создание топиков (запускается как kafka-init)
internal/
  user/ order/ product/ notification/   Бизнес-логика сервисов
  platform/             Каркас запуска, graceful shutdown
  testsupport/          Изолированные БД/Kafka для интеграционных тестов
migrations/             goose-миграции, embed (migrations.go)
pkg/
  auth/                 JWT (golang-jwt/v5), роли, bcrypt
  config/               Загрузка и валидация env-конфигурации
  events/               Контракт событий, Encode/Decode/PeekEnvelope
  httpx/                JSON-ответы, middleware (request-id, логи, recovery, JWT), сервер, пробы
  idempotency/          Таблица processed_events для идемпотентных консьюмеров
  kafkax/               Продюсер/консьюмер (segmentio/kafka-go): ретраи с backoff, DLQ, ручной commit
  logger/               log/slog в JSON
  outbox/               Transactional outbox: Enqueue(pgx.Tx) + релеер FOR UPDATE SKIP LOCKED
  postgres/             Пул pgx/v5, транзакции, goose-миграции
api/shop.http           Демо-сценарий HTTP-запросов
docker-compose.yaml     Вся инфраструктура и сервисы
Dockerfile              Один образ на все сервисы (build-arg SERVICE=...)
```
