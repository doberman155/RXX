-- +goose Up
-- Уведомления о смене статуса заказа. Реальная отправка письма в объём
-- работ не входит: уведомление сохраняется в таблицу и дублируется в лог.
CREATE TABLE IF NOT EXISTS notifications (
    id          UUID PRIMARY KEY,
    order_id    UUID NOT NULL,
    user_id     UUID NOT NULL,
    event_id    UUID NOT NULL,
    event_type  TEXT NOT NULL,
    kind        TEXT NOT NULL,
    old_status  TEXT,
    new_status  TEXT NOT NULL,
    message     TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Одно событие — не более одного уведомления даже при повторной доставке.
    CONSTRAINT notifications_event_key UNIQUE (event_id)
);

CREATE INDEX IF NOT EXISTS notifications_order_idx ON notifications (order_id, created_at);
CREATE INDEX IF NOT EXISTS notifications_user_idx ON notifications (user_id, created_at DESC);

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
DROP TABLE IF EXISTS notifications;
