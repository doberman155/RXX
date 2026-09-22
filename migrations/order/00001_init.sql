-- +goose Up
-- Заказы. Данные о товарах приезжают событиями от product-service,
-- прямых обращений в чужую базу нет.
CREATE TABLE IF NOT EXISTS orders (
    id                 UUID PRIMARY KEY,
    user_id            UUID NOT NULL,
    status             TEXT NOT NULL CHECK (status IN ('NEW', 'RESERVED', 'COMPLETED', 'CANCELLED')),
    total_amount_cents BIGINT NOT NULL DEFAULT 0 CHECK (total_amount_cents >= 0),
    currency           TEXT NOT NULL DEFAULT 'RUB',
    cancel_reason      TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS orders_user_idx ON orders (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS orders_status_idx ON orders (status);

CREATE TABLE IF NOT EXISTS order_items (
    id               UUID PRIMARY KEY,
    order_id         UUID NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    product_id       UUID NOT NULL,
    product_name     TEXT NOT NULL DEFAULT '',
    quantity         INTEGER NOT NULL CHECK (quantity > 0),
    -- Цена известна только после успешного резерва (приходит в stock.reserved).
    unit_price_cents BIGINT NOT NULL DEFAULT 0 CHECK (unit_price_cents >= 0),
    CONSTRAINT order_items_order_product_key UNIQUE (order_id, product_id)
);

CREATE INDEX IF NOT EXISTS order_items_order_idx ON order_items (order_id);

-- Повторный POST с тем же Idempotency-Key возвращает существующий заказ.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key          TEXT NOT NULL,
    user_id      UUID NOT NULL,
    request_hash TEXT NOT NULL,
    order_id     UUID NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, key)
);

-- Transactional outbox: событие пишется в одной транзакции с бизнес-данными.
CREATE TABLE IF NOT EXISTS outbox (
    id            BIGSERIAL PRIMARY KEY,
    event_id      UUID NOT NULL,
    topic         TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    payload       JSONB NOT NULL,
    headers       JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at       TIMESTAMPTZ,
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT
);

CREATE INDEX IF NOT EXISTS outbox_unsent_idx ON outbox (id) WHERE sent_at IS NULL;

-- Идентификаторы обработанных событий: повторная доставка не даёт второго эффекта.
CREATE TABLE IF NOT EXISTS processed_events (
    consumer_group TEXT NOT NULL,
    event_id       UUID NOT NULL,
    topic          TEXT NOT NULL,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, event_id)
);

-- +goose Down
DROP TABLE IF EXISTS processed_events;
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS idempotency_keys;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
