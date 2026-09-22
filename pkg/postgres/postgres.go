// Package postgres содержит подключение к PostgreSQL через pgx/v5,
// применение миграций (goose) и помощник для транзакций.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hshsb/shop/pkg/config"
)

// Pool — пул соединений; используется во всех репозиториях.
type Pool = pgxpool.Pool

// Connect открывает пул соединений и проверяет доступность базы.
// Ретраи нужны, потому что в docker compose сервис может стартовать
// раньше, чем PostgreSQL примет соединения.
func Connect(ctx context.Context, cfg config.Postgres) (*Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("разобрать POSTGRES_DSN: %w", err)
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.MaxConnLifetime = time.Hour
	poolCfg.MaxConnIdleTime = 30 * time.Minute
	poolCfg.HealthCheckPeriod = time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("создать пул соединений: %w", err)
	}

	if err := waitReady(ctx, pool, cfg.ConnectTimeout); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func waitReady(ctx context.Context, pool *Pool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	backoff := 200 * time.Millisecond

	for attempt := 1; ; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := pool.Ping(pingCtx)
		cancel()

		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("postgres недоступен после %d попыток: %w", attempt, err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}

// InTx выполняет fn внутри транзакции: коммитит при успехе и откатывает
// при ошибке или панике. Контекст обязателен и передаётся дальше.
func InTx(ctx context.Context, pool *Pool, fn func(ctx context.Context, tx pgx.Tx) error) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("открыть транзакцию: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			// Откатываем на отдельном контексте: исходный мог быть уже отменён.
			_ = tx.Rollback(context.WithoutCancel(ctx))
			panic(p)
		}
	}()

	if err := fn(ctx, tx); err != nil {
		if rbErr := tx.Rollback(context.WithoutCancel(ctx)); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			return errors.Join(err, fmt.Errorf("откатить транзакцию: %w", rbErr))
		}
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("зафиксировать транзакцию: %w", err)
	}
	return nil
}

// IsUniqueViolation сообщает, что ошибка — нарушение уникального индекса (23505).
func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// IsForeignKeyViolation сообщает о нарушении внешнего ключа (23503).
func IsForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23503"
}
