-- +goose Up
-- Пользователи: пароль хранится только в виде bcrypt-хеша.
CREATE TABLE IF NOT EXISTS users (
    id            UUID PRIMARY KEY,
    email         TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    role          TEXT NOT NULL CHECK (role IN ('USER', 'ADMIN')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Регистр e-mail не должен создавать дубликаты пользователей.
CREATE UNIQUE INDEX IF NOT EXISTS users_email_lower_key ON users (lower(email));

CREATE INDEX IF NOT EXISTS users_created_at_idx ON users (created_at DESC);

-- +goose Down
DROP INDEX IF EXISTS users_created_at_idx;
DROP INDEX IF EXISTS users_email_lower_key;
DROP TABLE IF EXISTS users;
