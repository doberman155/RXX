// Package idempotency обеспечивает однократный эффект от повторно
// доставленного события: идентификаторы обработанных событий хранятся в
// таблице processed_events в той же транзакции, что и бизнес-изменения.
package idempotency

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// MarkProcessed пытается отметить событие как обработанное.
// Возвращает false, если событие уже обрабатывалось этой consumer-группой —
// в этом случае обработчик должен завершиться без побочных эффектов.
//
// Вызов обязан выполняться в той же транзакции, что и бизнес-логика:
// иначе при откате транзакции событие останется помеченным.
func MarkProcessed(ctx context.Context, tx pgx.Tx, consumerGroup, eventID, topic string) (bool, error) {
	const query = `
		INSERT INTO processed_events (consumer_group, event_id, topic)
		VALUES ($1, $2, $3)
		ON CONFLICT (consumer_group, event_id) DO NOTHING`

	tag, err := tx.Exec(ctx, query, consumerGroup, eventID, topic)
	if err != nil {
		return false, fmt.Errorf("отметить событие %s обработанным: %w", eventID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// WasProcessed проверяет, обрабатывалось ли событие ранее.
// Используется в тестах и диагностике; в рабочем пути достаточно MarkProcessed.
func WasProcessed(ctx context.Context, q Querier, consumerGroup, eventID string) (bool, error) {
	const query = `SELECT EXISTS (SELECT 1 FROM processed_events WHERE consumer_group = $1 AND event_id = $2)`

	var exists bool
	if err := q.QueryRow(ctx, query, consumerGroup, eventID).Scan(&exists); err != nil {
		return false, fmt.Errorf("проверить обработку события %s: %w", eventID, err)
	}
	return exists, nil
}

// Querier — общий интерфейс пула и транзакции pgx.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}
