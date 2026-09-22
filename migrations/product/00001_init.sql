-- +goose Up
-- Каталог товаров. Остаток на складе хранится вместе с товаром:
-- отдельного сервиса склада в проекте нет.
CREATE TABLE IF NOT EXISTS products (
    id          UUID PRIMARY KEY,
    sku         TEXT NOT NULL,
    name        TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    category    TEXT NOT NULL,
    price_cents BIGINT NOT NULL CHECK (price_cents >= 0),
    stock       INTEGER NOT NULL CHECK (stock >= 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at  TIMESTAMPTZ
);

-- SKU уникален среди живых товаров: удалённый товар не блокирует новый.
CREATE UNIQUE INDEX IF NOT EXISTS products_sku_key ON products (sku) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS products_category_idx ON products (category) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS products_created_at_idx ON products (created_at DESC);

-- Брони под заказ. Нужны для компенсации: по order.cancelled остаток возвращается.
CREATE TABLE IF NOT EXISTS reservations (
    id          UUID PRIMARY KEY,
    order_id    UUID NOT NULL,
    product_id  UUID NOT NULL REFERENCES products (id),
    quantity    INTEGER NOT NULL CHECK (quantity > 0),
    status      TEXT NOT NULL CHECK (status IN ('HELD', 'RELEASED')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at TIMESTAMPTZ,
    CONSTRAINT reservations_order_product_key UNIQUE (order_id, product_id)
);

CREATE INDEX IF NOT EXISTS reservations_order_idx ON reservations (order_id);

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
DROP TABLE IF EXISTS reservations;
DROP TABLE IF EXISTS products;
