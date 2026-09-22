# Событийное взаимодействие сервисов на Go + Kafka

Учебный монорепозиторий по ТЗ: обработка заказов интернет-магазина на четырёх
независимых сервисах. Между сервисами нет синхронных вызовов — только события
Kafka. Стек намеренно минимальный: **Go 1.26, Apache Kafka (Redpanda),
PostgreSQL, Docker Compose**.

Реализовано: transactional outbox, идемпотентная обработка событий, сага
резервирования с компенсацией, ретраи с dead-letter topic, JWT-аутентификация
и graceful shutdown по SIGTERM.

---

## Содержание

- [Быстрый старт](#быстрый-старт)
- [Архитектура](#архитектура)
- [REST-API](#rest-api)
- [События Kafka](#события-kafka)
- [Надёжность](#надёжность)
- [Хранение данных](#хранение-данных)
- [Переменные окружения](#переменные-окружения)
- [Разработка и тесты](#разработка-и-тесты)
- [Структура репозитория](#структура-репозитория)
- [Что не входит в объём работ](#что-не-входит-в-объём-работ)

---

## Быстрый старт

Требуется Windows 10/11 с Docker Desktop (WSL 2 backend), 2 CPU, 8 ГБ RAM.
Для сборки и тестов вне контейнеров — Go 1.26+.

Весь стек поднимается **одной командой** — она одинаково работает в PowerShell,
WSL2 и Git Bash:

```bash
docker compose up -d --build
```

Команда поднимает Redpanda и PostgreSQL, создаёт топики (по 3 партиции на топик
плюс их DLQ — это делает одноразовый сервис `kafka-init`) и запускает четыре
сервиса. Миграции применяются при старте каждого сервиса.

Проверка готовности — все четыре должны ответить `{"status":"ok"}`:

```bash
curl http://localhost:8081/healthz
```

Состояние контейнеров и healthcheck:

```bash
docker compose ps
```

Остановка (`-v` удаляет тома с данными; без флага база и топики сохранятся):

```bash
docker compose down -v
```

### Демонстрационный сценарий

Примеры всех запросов — в [`api/shop.http`](api/shop.http) (JetBrains HTTP
Client или VS Code REST Client). Порядок в файле повторяет сценарий из ТЗ:

1. вход администратора (`admin@shop.local` / `admin_local_password`);
2. создание товаров;
3. регистрация покупателя и вход;
4. заказ: `NEW → RESERVED → COMPLETED` без ручных действий;
5. повтор `POST /orders` с тем же `Idempotency-Key` — возвращается тот же заказ;
6. заказ с нехваткой товара: `NEW → CANCELLED`, остаток не меняется;
7. уведомления, сохранённые `notification-service`.

Логи саги:

```bash
docker compose logs -f order-service product-service notification-service
```

### Порты

| Сервис | REST | Назначение |
|---|---|---|
| user-service | http://localhost:8081 | регистрация, вход, JWT, роли |
| product-service | http://localhost:8082 | каталог, остатки, резерв по событиям |
| order-service | http://localhost:8083 | заказы, статусы, оркестрация саги |
| notification-service | http://localhost:8084 | уведомления о смене статуса |
| Redpanda (Kafka) | localhost:19092 | внешний listener для хоста |
| PostgreSQL | localhost:5433 | 5433, чтобы не конфликтовать с локальным сервером |

---

## Архитектура

```
                 REST (JWT)                        Kafka
  ┌────────┐    ┌──────────────┐
  │ Клиент │───▶│ user-service │  выпускает JWT, роли USER / ADMIN
  └───┬────┘    └──────────────┘
      │
      │         ┌───────────────┐   order.created    ┌──────────────────┐
      ├────────▶│ order-service │──────────────────▶ │ product-service  │
      │         │               │◀────────────────── │                  │
      │         │  сага, outbox │  stock.reserved    │ каталог, остатки │
      │         │               │  stock.rejected    │                  │
      │         │               │──────────────────▶ │                  │
      │         └───────┬───────┘  order.cancelled   └──────────────────┘
      │                 │           (компенсация)
      │                 │ order.status-changed
      │                 ▼
      │         ┌──────────────────────┐
      └────────▶│ notification-service │  сохраняет уведомления
                └──────────────────────┘
```

Принципы, заданные ТЗ:

- **Нет синхронных вызовов между сервисами.** Если сервису нужны чужие данные,
  он получает их из событий. Например, `order-service` не знает цен: они
  приезжают в `stock.reserved` и сохраняются в позициях заказа.
- **Нет api-gateway.** Клиент ходит в сервисы напрямую; `user-service` выпускает
  JWT, остальные проверяют подпись своим middleware по общему секрету.
- **Монорепо.** Точка входа каждого сервиса — `cmd/<service>`, внутренняя
  логика — `internal/<домен>`, общий код — `pkg/`.
- **Единое поведение при остановке.** Каждый сервис отдаёт `/healthz` и по
  SIGTERM перестаёт принимать новое, дорабатывает текущее и закрывает соединения.

### Состав сервисов

| Сервис | Назначение | Хранилище | Взаимодействие |
|---|---|---|---|
| `user-service` | Регистрация, вход, выпуск JWT, роли USER/ADMIN | PostgreSQL (`shop_user`) | REST |
| `product-service` | Каталог товаров, остатки, резервирование под заказ | PostgreSQL (`shop_product`) | REST, Kafka |
| `order-service` | Создание заказа, статусы, оркестрация саги | PostgreSQL (`shop_order`) | REST, Kafka |
| `notification-service` | Уведомления о смене статуса заказа | PostgreSQL (`shop_notification`) | Kafka |

### Технологический набор

| Область | Инструменты |
|---|---|
| HTTP/REST | `net/http` со стандартным `ServeMux` (шаблоны Go 1.22+); middleware: request-id, логирование, recovery, проверка JWT |
| Kafka | `segmentio/kafka-go`; consumer-группы, ручной commit offset |
| PostgreSQL | `pgx/v5`, миграции — `goose` (встроены в бинарник через `embed`) |
| Аутентификация | `golang-jwt/jwt/v5`, пароли — `bcrypt` |
| Логи | `log/slog` в формате JSON |
| Тесты | `testify`, интеграционные — против PostgreSQL и Kafka из docker compose |
| Запуск | Docker Compose (Redpanda — один контейнер, без ZooKeeper) |

---

## REST-API

Все ответы — JSON. Ошибки приходят в едином формате:

```json
{
  "error": {
    "code": "validation_error",
    "message": "пароль короче 8 символов",
    "fields": { "password": "пароль короче 8 символов" },
    "request_id": "0b1f…"
  }
}
```

Коды: `bad_request`, `validation_error`, `unauthorized`, `forbidden`,
`not_found`, `conflict`, `internal_error`.

### user-service (8081)

| Метод и путь | Доступ | Описание |
|---|---|---|
| `POST /api/v1/auth/register` | все | Регистрация по e-mail и паролю. Роль всегда `USER` |
| `POST /api/v1/auth/login` | все | Вход, в ответ — JWT со сроком жизни и ролью |
| `GET /api/v1/users/me` | JWT | Текущий пользователь |
| `GET /api/v1/users?limit&offset` | ADMIN | Список пользователей |
| `GET /api/v1/users/{id}` | ADMIN | Пользователь по идентификатору |
| `GET /healthz`, `GET /readyz` | все | Живость и готовность |

Регистрация всегда выдаёт роль `USER`, поэтому первый администратор создаётся
при старте сервиса из `BOOTSTRAP_ADMIN_EMAIL` и `BOOTSTRAP_ADMIN_PASSWORD`.

### product-service (8082)

| Метод и путь | Доступ | Описание |
|---|---|---|
| `GET /api/v1/products?category&limit&offset` | все | Каталог с фильтром по категории и пагинацией |
| `GET /api/v1/products/{id}` | все | Карточка товара |
| `POST /api/v1/products` | ADMIN | Создание товара |
| `PUT /api/v1/products/{id}` | ADMIN | Полное обновление |
| `DELETE /api/v1/products/{id}` | ADMIN | Удаление (мягкое: на товар могут ссылаться брони) |
| `GET /healthz`, `GET /readyz` | все | Живость и готовность |

Резервирование и освобождение остатка выполняются **только по событиям** от
`order-service` — REST-ручек для этого нет.

Цены хранятся в копейках (`price_cents`), валюта одна — `RUB`.

### order-service (8083)

| Метод и путь | Доступ | Описание |
|---|---|---|
| `POST /api/v1/orders` | USER/ADMIN | Создание заказа. Требуется заголовок `Idempotency-Key` |
| `GET /api/v1/orders?status&limit&offset` | JWT | Свои заказы; ADMIN видит все |
| `GET /api/v1/orders/{id}` | JWT | Заказ по идентификатору |
| `POST /api/v1/orders/{id}/complete` | JWT | `RESERVED → COMPLETED` |
| `POST /api/v1/orders/{id}/cancel` | JWT | Отмена; из `RESERVED` публикует компенсацию |
| `GET /healthz`, `GET /readyz` | все | Живость и готовность |

Пример создания заказа:

```http
POST /api/v1/orders
Authorization: Bearer <token>
Idempotency-Key: 6f1b0e6c-1a5e-4c9f-9b2e-0a1d2c3b4a55
Content-Type: application/json

{ "items": [ { "product_id": "…", "quantity": 2 } ] }
```

Повторный `POST` с тем же ключом возвращает **200** и существующий заказ вместо
**201**. Тот же ключ с другим составом заказа — **409**.

Чужой заказ отдаётся как `404`, а не `403`: так не утекает сам факт его
существования.

### notification-service (8084)

| Метод и путь | Доступ | Описание |
|---|---|---|
| `GET /api/v1/notifications?order_id&user_id&limit&offset` | JWT | Свои уведомления; ADMIN видит все |
| `GET /healthz`, `GET /readyz` | все | Живость и готовность |

Данные сервис получает только из Kafka; HTTP нужен для проб и для просмотра
сохранённых уведомлений при демонстрации.

---

## События Kafka

| Топик | Ключ | Формат | Producer | Consumer(s) |
|---|---|---|---|---|
| `order.created` | orderId | JSON | order-service | product-service, notification-service |
| `stock.reserved` | orderId | JSON | product-service | order-service |
| `stock.rejected` | orderId | JSON | product-service | order-service |
| `order.cancelled` | orderId | JSON | order-service | product-service |
| `order.status-changed` | orderId | JSON | order-service | notification-service |

К каждому топику создаётся dead-letter очередь `<topic>.dlq`.
Топики создаёт отдельная команда `kafka-admin` — по **3 партиции** на топик
(у DLQ одна: порядок там не важен). В compose она запускается одноразовым
сервисом `kafka-init`, пересоздать топики вручную можно так:

```bash
docker compose run --rm kafka-init
```

Вызов идемпотентен: существующие топики пропускаются.

**Ключ сообщения — `orderId`.** Это гарантирует, что все события одного заказа
попадают в одну партицию и обрабатываются по порядку. Schema Registry не
используется: структуры событий описаны в [`pkg/events`](pkg/events/events.go).

### Общая часть события

У каждого события есть `event_id`, `occurred_at` и `order_id`:

```json
{
  "event_id": "1f9a2b63-3f0e-4a2a-9c3a-6a1f2c5d7e01",
  "event_type": "order.created",
  "occurred_at": "2026-09-16T14:35:27.921740Z",
  "order_id": "7f3f4c61-6191-4749-b9fe-901158c246c5"
}
```

### Полезная нагрузка

<details>
<summary><code>order.created</code></summary>

```json
{
  "event_id": "…", "event_type": "order.created",
  "occurred_at": "2026-09-16T14:35:27.921740Z",
  "order_id": "7f3f4c61-…",
  "user_id": "41daf956-…",
  "items": [ { "product_id": "9c957f22-…", "quantity": 2 } ]
}
```
</details>

<details>
<summary><code>stock.reserved</code> — несёт цены на момент резерва</summary>

```json
{
  "event_id": "…", "event_type": "stock.reserved",
  "occurred_at": "…", "order_id": "7f3f4c61-…",
  "items": [
    { "product_id": "9c957f22-…", "name": "Кофемолка ручная",
      "quantity": 2, "unit_price_cents": 599000 }
  ],
  "total_amount_cents": 1198000,
  "currency": "RUB"
}
```
</details>

<details>
<summary><code>stock.rejected</code></summary>

```json
{
  "event_id": "…", "event_type": "stock.rejected",
  "occurred_at": "…", "order_id": "7f3f4c61-…",
  "reason": "INSUFFICIENT_STOCK",
  "message": "недостаточно товара на складе",
  "items": [ { "product_id": "e2c58d8f-…", "requested": 99, "available": 1 } ]
}
```

`reason` принимает значения `INSUFFICIENT_STOCK` и `PRODUCT_NOT_FOUND`.
</details>

<details>
<summary><code>order.cancelled</code> — компенсирующее событие</summary>

```json
{
  "event_id": "…", "event_type": "order.cancelled",
  "occurred_at": "…", "order_id": "7f3f4c61-…",
  "user_id": "41daf956-…",
  "previous_status": "RESERVED",
  "reason": "покупатель передумал",
  "stock_was_reserved": true
}
```
</details>

<details>
<summary><code>order.status-changed</code></summary>

```json
{
  "event_id": "…", "event_type": "order.status-changed",
  "occurred_at": "…", "order_id": "7f3f4c61-…",
  "user_id": "41daf956-…",
  "old_status": "NEW",
  "new_status": "RESERVED",
  "total_amount_cents": 1198000,
  "reason": ""
}
```
</details>

### Сага резервирования

```
POST /orders ──▶ NEW ──order.created──▶ product-service
                                            │
                     ┌──────────────────────┴──────────────────────┐
                     │ остатка хватает                             │ не хватает
                     ▼                                             ▼
               stock.reserved                                stock.rejected
                     │                                             │
                     ▼                                             ▼
                 RESERVED ──(ORDER_AUTO_COMPLETE)──▶ COMPLETED  CANCELLED
                     │
                     └── POST /orders/{id}/cancel ──▶ CANCELLED
                                                          │
                                                   order.cancelled
                                                          │
                                                          ▼
                                          product-service возвращает остаток
```

Переходы статусов описаны явно; всё, чего нет в таблице, отклоняется с 409:

| Из | В |
|---|---|
| `NEW` | `RESERVED`, `CANCELLED` |
| `RESERVED` | `COMPLETED`, `CANCELLED` (с компенсацией остатка) |
| `COMPLETED`, `CANCELLED` | терминальные |

**`ORDER_AUTO_COMPLETE` (по умолчанию `true`)** переводит заказ
`RESERVED → COMPLETED` сразу после успешного резерва — тогда путь
`NEW → RESERVED → COMPLETED` проходит без ручных действий, как требует ТЗ.
Чтобы вручную посмотреть компенсацию остатка, поднимите стек с
`ORDER_AUTO_COMPLETE=false`: заказ остановится в `RESERVED`, и его можно
завершить (`/complete`) или отменить (`/cancel`) самому. Оба режима покрыты
интеграционными тестами.

---

## Надёжность

### Transactional outbox

Событие пишется в таблицу `outbox` **в той же транзакции**, что и бизнес-данные.
Отдельная горутина-релеер вычитывает неотправленные записи
(`FOR UPDATE SKIP LOCKED`, по возрастанию `id`), публикует их в Kafka и
проставляет `sent_at`. Прямая публикация из HTTP-обработчика не допускается:
`pkg/outbox.Enqueue` принимает только `pgx.Tx`.

Записи забираются по возрастанию `id` и публикуются одним батчем, поэтому
порядок событий одного заказа сохраняется. Релеер рассчитан на **один экземпляр
сервиса**: `SKIP LOCKED` защищает от дублей, но строгий порядок для одного
ключа гарантируется только при одной реплике.

Доставка — at-least-once: если публикация прошла, а коммит не успел, событие
уедет повторно. Это безопасно, потому что все консьюмеры идемпотентны.

### Идемпотентность

- **События.** Перед обработкой consumer-группа пишет `event_id` в
  `processed_events` в той же транзакции, что и бизнес-изменения. Конфликт по
  первичному ключу означает дубликат — обработчик выходит без побочных эффектов.
  `notification-service` дополнительно защищён уникальным индексом по `event_id`.
- **REST.** `POST /orders` требует `Idempotency-Key`. Ключ хранится вместе с
  хешем состава заказа: повтор возвращает существующий заказ, а тот же ключ с
  другим составом — 409. Хеш не зависит от порядка позиций.
- **Резерв.** Уникальный индекс `(order_id, product_id)` в `reservations` не даёт
  зарезервировать один заказ дважды даже при перепубликации события.

### Ретраи и dead-letter topic

При ошибке обработка повторяется с нарастающей задержкой
(`KAFKA_RETRY_BACKOFF`, удвоение до `KAFKA_MAX_RETRY_BACKOFF`). После
`KAFKA_MAX_RETRIES` попыток сообщение уходит в `<topic>.dlq` вместе с
заголовками причины и коммитится, чтобы не блокировать остальные:

| Заголовок | Значение |
|---|---|
| `x-original-topic`, `x-original-partition`, `x-original-offset` | откуда пришло |
| `x-error` | текст последней ошибки |
| `x-attempts` | сколько попыток сделано |
| `x-failed-at` | время отправки в DLQ |

Ошибки, которые повторять бессмысленно (битый JSON, неизвестный топик),
помечаются `kafkax.NonRetryable` и уезжают в DLQ сразу, без повторов.

Посмотреть DLQ:

```bash
docker exec shop-redpanda rpk topic consume order.created.dlq --num 5 --offset start
```

### Порядок и consumer-группы

Каждый консьюмер работает в своей группе (`<service>.<назначение>`) и коммитит
offset **только после успешной обработки** (`CommitInterval: 0`, явный
`CommitMessages`). Если сервис остановили во время обработки, offset не
коммитится и сообщение придёт повторно — идемпотентность гасит повтор.

### Graceful shutdown

По SIGTERM контекст отменяется, и все компоненты (`http`, `kafka-consumer`,
`outbox-relayer`) останавливаются параллельно:

- HTTP перестаёт принимать соединения и даёт активным запросам доработать;
- консьюмер не забирает новые сообщения, **дорабатывает текущее** (обработка идёт
  на контексте, переживающем отмену) и коммитит offset;
- если уложиться в `SHUTDOWN_TIMEOUT` не удалось, процесс завершается с ошибкой,
  а не висит.

Проверено тестами `internal/platform` и `pkg/httpx`, а также вручную:
остановка и повторный запуск любого сервиса в середине сценария не теряет
событий и не ломает заказ.

---

## Хранение данных

Единственное хранилище — PostgreSQL. **У каждого сервиса своя база**
(`shop_user`, `shop_product`, `shop_order`, `shop_notification`), обращаться к
чужим таблицам нельзя. Базы создаются при первом старте контейнера
(`deploy/postgres/init-databases.sh`).

Миграции — `goose`, встроены в бинарник через `embed` и применяются при старте
сервиса под advisory-lock (`POSTGRES_MIGRATE_ON_START=false` отключает это).
Транзакции открываются явно и всегда получают `context`.

| База | Таблицы |
|---|---|
| `shop_user` | `users` |
| `shop_product` | `products`, `reservations`, `outbox`, `processed_events` |
| `shop_order` | `orders`, `order_items`, `idempotency_keys`, `outbox`, `processed_events` |
| `shop_notification` | `notifications`, `processed_events` |

Подключиться к базе:

```bash
docker compose exec postgres psql -U shop -d shop_order
```

---

## Переменные окружения

Значения стенда лежат в `.env` (в репозитории — только локальные, не секреты).

### Общие

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `HTTP_PORT` | по сервису | Порт REST |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `SHUTDOWN_TIMEOUT` | `20s` | Предел корректного завершения |
| `POSTGRES_DSN` | — (обязательна) | Строка подключения к своей базе |
| `POSTGRES_MAX_CONNS` | `10` | Размер пула |
| `POSTGRES_MIGRATE_ON_START` | `true` | Применять миграции при старте |
| `KAFKA_BROKERS` | `localhost:9092` | Список брокеров через запятую |
| `KAFKA_GROUP_ID` | имя сервиса | Базовое имя consumer-группы |
| `KAFKA_MAX_RETRIES` | `3` | Попыток обработки до DLQ |
| `KAFKA_RETRY_BACKOFF` | `500ms` | Стартовая задержка повтора |
| `KAFKA_MAX_RETRY_BACKOFF` | `10s` | Предел задержки |
| `KAFKA_DLQ_SUFFIX` | `.dlq` | Суффикс dead-letter топика |
| `KAFKA_PARTITIONS` | `3` | Партиций при создании топиков |
| `JWT_SECRET` | — (обязательна, ≥16 символов) | Общий секрет подписи |
| `JWT_TTL` | `24h` | Срок жизни токена |
| `JWT_ISSUER` | `user-service` | Издатель токена |
| `OUTBOX_POLL_INTERVAL` | `1s` | Период опроса outbox |
| `OUTBOX_BATCH_SIZE` | `100` | Размер пачки релеера |

### Отдельных сервисов

| Переменная | Сервис | По умолчанию | Назначение |
|---|---|---|---|
| `ORDER_AUTO_COMPLETE` | order | `true` | Автопереход `RESERVED → COMPLETED` |
| `BCRYPT_COST` | user | `10` | Стоимость хеширования пароля |
| `BOOTSTRAP_ADMIN_EMAIL` | user | — | Администратор стенда |
| `BOOTSTRAP_ADMIN_PASSWORD` | user | — | Пароль администратора |

Сервис на старте проверяет всю конфигурацию сразу и сообщает обо **всех**
проблемах одним сообщением. Пароли и токены в логи не пишутся.

---

## Разработка и тесты

Сборка и проверки:

```bash
go build ./...
go vet ./...
gofmt -l .
```

Тесты:

```bash
go test ./... -race -count=1
```

```bash
go test -tags=integration ./... -race -count=1
```

Покрытие (порог по ТЗ — 60 %):

```bash
go test -tags=integration ./... -count=1 -coverprofile=coverage.out -coverpkg=./internal/...,./pkg/... && go tool cover -func=coverage.out | tail -1
```

Собрать бинарник конкретного сервиса:

```bash
go build -trimpath -o bin/order-service ./cmd/order-service
```

Полезные команды стенда:

```bash
docker compose logs -f order-service product-service notification-service
```

```bash
docker compose exec postgres psql -U shop -d shop_order
```

### Интеграционные тесты

Собираются по тегу `integration` и идут против настоящих PostgreSQL и Kafka из
`docker compose`. Каждый тест создаёт **отдельную базу**, накатывает миграции и
удаляет её за собой, поэтому тесты независимы. Если инфраструктура недоступна,
тесты не падают, а помечаются `skip`.

Адреса переопределяются переменными:

```bash
TEST_POSTGRES_DSN=postgres://shop:shop_local_password@localhost:5433/postgres?sslmode=disable
TEST_KAFKA_BROKERS=localhost:19092
```

Что покрыто:

- REST каждого сервиса целиком (включая коды ошибок, роли и доступ к чужим данным);
- сага: успешный резерв, отказ, компенсация, недопустимые переходы;
- **повторная доставка одного сообщения не создаёт второй эффект** — отдельные
  тесты в `internal/product`, `internal/order`, `internal/notification`;
- outbox-релеер: публикация, пометка `sent_at`, отсутствие повторной отправки;
- ретраи, dead-letter topic и неповторяемые ошибки (`test/integration`);
- все события одного заказа попадают в одну партицию и сохраняют порядок;
- graceful shutdown HTTP-сервера и компонентов сервиса.

Текущее покрытие (юнит + интеграционные): **76.5 %** при требуемых 60 %.

> `-race` требует cgo. На Windows без установленного gcc используйте
> `go test ./...` без флага либо запускайте тесты в WSL2.

---

## Структура репозитория

```
cmd/                      точки входа
  user-service/           каждый сервис — отдельный бинарник
  product-service/
  order-service/
  notification-service/
  kafka-admin/            создание топиков (сервис kafka-init в compose)

internal/                 внутренняя логика, недоступная извне модуля
  user/                   model, repository, service, handler, app
  product/                + consumer: обработка order.created / order.cancelled
  order/                  + сага и переходы статусов
  notification/           + consumer: order.created / order.status-changed
  platform/               общий каркас запуска и graceful shutdown
  testsupport/            изолированная база и Kafka для интеграционных тестов

pkg/                      общий код сервисов
  config/                 загрузка конфигурации из окружения
  logger/                 log/slog в JSON
  httpx/                  ответы, middleware, сервер, пробы
  auth/                   выпуск и проверка JWT, роли
  postgres/               пул pgx, транзакции, миграции goose
  events/                 контракт событий Kafka
  kafkax/                 продюсер, консьюмер с ретраями и DLQ, топики
  outbox/                 transactional outbox и релеер
  idempotency/            processed_events

migrations/               SQL-миграции, встроены через embed
deploy/postgres/          создание баз при первом старте контейнера
api/shop.http             примеры запросов ко всем сервисам
test/integration/         тесты Kafka: outbox, ретраи, DLQ, порядок
```

Каждый сервис устроен одинаково: `model` (типы и правила предметной области) →
`repository` (SQL) → `service` (бизнес-логика и транзакции) → `handler` (REST)
и `consumer` (события), а `app.go` связывает всё вместе.

---

## Что не входит в объём работ

По ТЗ намеренно исключены: gRPC, Kubernetes, MongoDB, Redis, OAuth, Schema
Registry, распределённая трассировка, фронтенд, подтверждение почты и
refresh-токены, реальная отправка писем, деплой и Helm.

Stretch goals (строго в этом порядке): Redis для кэша и rate-limiting; gRPC
вместо части событий; Prometheus + Grafana с метриками консьюмеров; вынесение
склада в отдельный `inventory-service`; деплой в k3d через Helm.
