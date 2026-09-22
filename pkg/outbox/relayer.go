package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/hshsb/shop/pkg/config"
	"github.com/hshsb/shop/pkg/kafkax"
	"github.com/hshsb/shop/pkg/logger"
	"github.com/hshsb/shop/pkg/postgres"
)

// Relayer периодически вычитывает неотправленные записи outbox и
// публикует их в Kafka. Порядок сохраняется: записи забираются по
// возрастанию id и публикуются одним батчем.
//
// Релеер рассчитан на один экземпляр сервиса: FOR UPDATE SKIP LOCKED
// защищает от дублей при нескольких репликах, но строгий порядок
// сообщений одного ключа гарантируется только при одной реплике.
type Relayer struct {
	pool     *postgres.Pool
	producer *kafkax.Producer
	cfg      config.Outbox
	log      *slog.Logger
}

// NewRelayer создаёт релеер.
func NewRelayer(pool *postgres.Pool, producer *kafkax.Producer, cfg config.Outbox, log *slog.Logger) *Relayer {
	return &Relayer{pool: pool, producer: producer, cfg: cfg, log: log.With(slog.String("component", "outbox-relayer"))}
}

// Run работает до отмены контекста. Текущая пачка всегда допубликовывается,
// чтобы не потерять уже прочитанные записи.
func (r *Relayer) Run(ctx context.Context) error {
	r.log.Info("релеер outbox запущен", slog.Duration("poll_interval", r.cfg.PollInterval))

	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	cleanup := time.NewTicker(time.Hour)
	defer cleanup.Stop()

	for {
		select {
		case <-ctx.Done():
			r.log.Info("релеер outbox остановлен")
			return nil
		case <-ticker.C:
			r.drain(ctx)
		case <-cleanup.C:
			r.cleanupSent(ctx)
		}
	}
}

// drain публикует накопившиеся записи, пока они есть.
func (r *Relayer) drain(ctx context.Context) {
	for {
		sent, err := r.relayBatch(ctx)
		if err != nil {
			if ctx.Err() == nil {
				r.log.Error("не удалось отправить пачку outbox", slog.String("error", err.Error()))
			}
			return
		}
		if sent < r.cfg.BatchSize {
			return
		}
		if ctx.Err() != nil {
			return
		}
	}
}

type outboxRow struct {
	id      int64
	eventID string
	topic   string
	key     string
	payload []byte
	headers map[string]string
}

// relayBatch забирает пачку записей, публикует их и помечает отправленными
// в одной транзакции: повторной отправки не будет, если коммит прошёл.
func (r *Relayer) relayBatch(ctx context.Context) (int, error) {
	// Публикация выполняется внутри транзакции с блокировкой строк, поэтому
	// параллельный релеер не возьмёт те же записи (FOR UPDATE SKIP LOCKED).
	var published int

	err := postgres.InTx(ctx, r.pool, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := r.claim(ctx, tx)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}

		msgs := make([]kafkax.Message, 0, len(rows))
		ids := make([]int64, 0, len(rows))
		for _, row := range rows {
			msgs = append(msgs, kafkax.Message{
				Topic:   row.topic,
				Key:     row.key,
				Value:   row.payload,
				Headers: row.headers,
			})
			ids = append(ids, row.id)
		}

		// Публикуем на контексте, переживающем отмену: пачка уже заблокирована
		// в БД, и её нужно довести до Kafka, иначе события задержатся.
		pubCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()

		if pubErr := r.producer.Publish(pubCtx, msgs...); pubErr != nil {
			if markErr := r.markFailed(ctx, tx, ids, pubErr); markErr != nil {
				return errors.Join(pubErr, markErr)
			}
			// Возвращаем nil: транзакция коммитится, чтобы сохранить счётчик
			// попыток, а сами записи останутся неотправленными.
			published = 0
			return nil
		}

		if err := r.markSent(ctx, tx, ids); err != nil {
			return err
		}

		for _, row := range rows {
			logger.WithOrderID(r.log, row.key).Debug("событие опубликовано",
				slog.String(logger.KeyEventID, row.eventID),
				slog.String(logger.KeyTopic, row.topic),
			)
		}

		published = len(rows)
		return nil
	})
	if err != nil {
		return 0, err
	}

	if published > 0 {
		r.log.Info("отправлена пачка событий из outbox", slog.Int("count", published))
	}
	return published, nil
}

func (r *Relayer) claim(ctx context.Context, tx pgx.Tx) ([]outboxRow, error) {
	const query = `
		SELECT id, event_id, topic, partition_key, payload, headers
		FROM outbox
		WHERE sent_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`

	rows, err := tx.Query(ctx, query, r.cfg.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("выбрать записи outbox: %w", err)
	}
	defer rows.Close()

	var out []outboxRow
	for rows.Next() {
		var (
			row        outboxRow
			rawHeaders []byte
		)
		if err := rows.Scan(&row.id, &row.eventID, &row.topic, &row.key, &row.payload, &rawHeaders); err != nil {
			return nil, fmt.Errorf("прочитать запись outbox: %w", err)
		}
		if len(rawHeaders) > 0 {
			if err := json.Unmarshal(rawHeaders, &row.headers); err != nil {
				return nil, fmt.Errorf("разобрать заголовки записи outbox %d: %w", row.id, err)
			}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("обойти записи outbox: %w", err)
	}
	return out, nil
}

func (r *Relayer) markSent(ctx context.Context, tx pgx.Tx, ids []int64) error {
	const query = `UPDATE outbox SET sent_at = now(), attempts = attempts + 1, last_error = NULL WHERE id = ANY($1)`
	if _, err := tx.Exec(ctx, query, ids); err != nil {
		return fmt.Errorf("пометить записи outbox отправленными: %w", err)
	}
	return nil
}

func (r *Relayer) markFailed(ctx context.Context, tx pgx.Tx, ids []int64, cause error) error {
	const query = `
		UPDATE outbox
		SET attempts = attempts + 1, last_error = $2
		WHERE id = ANY($1)
		RETURNING id, attempts`

	rows, err := tx.Query(ctx, query, ids, cause.Error())
	if err != nil {
		return fmt.Errorf("обновить счётчик попыток outbox: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id       int64
			attempts int
		)
		if err := rows.Scan(&id, &attempts); err != nil {
			return fmt.Errorf("прочитать результат обновления outbox: %w", err)
		}
		if attempts >= r.cfg.MaxAttempts {
			r.log.Error("событие из outbox долго не удаётся отправить",
				slog.Int64("outbox_id", id),
				slog.Int("attempts", attempts),
				slog.String("error", cause.Error()),
			)
		}
	}
	return rows.Err()
}

// cleanupSent удаляет старые отправленные записи, чтобы таблица не росла.
func (r *Relayer) cleanupSent(ctx context.Context) {
	const query = `DELETE FROM outbox WHERE sent_at IS NOT NULL AND sent_at < now() - $1::interval`

	tag, err := r.pool.Exec(ctx, query, r.cfg.CleanupAfter.String())
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("не удалось очистить отправленные записи outbox", slog.String("error", err.Error()))
		}
		return
	}
	if n := tag.RowsAffected(); n > 0 {
		r.log.Info("очищены отправленные записи outbox", slog.Int64("count", n))
	}
}
