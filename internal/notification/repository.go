package notification

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/hshsb/shop/pkg/postgres"
)

// Repository — доступ к таблице notifications.
type Repository struct {
	pool *postgres.Pool
}

// NewRepository создаёт репозиторий.
func NewRepository(pool *postgres.Pool) *Repository { return &Repository{pool: pool} }

// Pool возвращает пул: нужен сервису для транзакций.
func (r *Repository) Pool() *postgres.Pool { return r.pool }

// Ping используется readiness-пробой.
func (r *Repository) Ping(ctx context.Context) error { return r.pool.Ping(ctx) }

// Insert сохраняет уведомление внутри переданной транзакции.
// Повторная доставка того же события не создаёт второе уведомление:
// уникальный индекс по event_id гасит дубликат.
func (r *Repository) Insert(ctx context.Context, tx pgx.Tx, n Notification) (bool, error) {
	const query = `
		INSERT INTO notifications (id, order_id, user_id, event_id, event_type, kind, old_status, new_status, message)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8, $9)
		ON CONFLICT (event_id) DO NOTHING`

	tag, err := tx.Exec(ctx, query, n.ID, n.OrderID, n.UserID, n.EventID, n.EventType,
		n.Kind, n.OldStatus, n.NewStatus, n.Message)
	if err != nil {
		return false, fmt.Errorf("сохранить уведомление: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// List возвращает страницу уведомлений с фильтром по заказу и пользователю.
func (r *Repository) List(ctx context.Context, f ListFilter) ([]Notification, int, error) {
	const query = `
		SELECT id, order_id, user_id, event_id, event_type, kind,
		       coalesce(old_status, ''), new_status, message, created_at,
		       count(*) OVER () AS total
		FROM notifications
		WHERE ($1::uuid IS NULL OR order_id = $1)
		  AND ($2::uuid IS NULL OR user_id = $2)
		ORDER BY created_at DESC, id
		LIMIT $3 OFFSET $4`

	rows, err := r.pool.Query(ctx, query, f.OrderID, f.UserID, f.Limit, f.Offset)
	if err != nil {
		return nil, 0, fmt.Errorf("получить уведомления: %w", err)
	}
	defer rows.Close()

	var (
		items []Notification
		total int
	)
	for rows.Next() {
		var n Notification
		if err := rows.Scan(&n.ID, &n.OrderID, &n.UserID, &n.EventID, &n.EventType, &n.Kind,
			&n.OldStatus, &n.NewStatus, &n.Message, &n.CreatedAt, &total); err != nil {
			return nil, 0, fmt.Errorf("прочитать уведомление: %w", err)
		}
		items = append(items, n)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("обойти уведомления: %w", err)
	}
	return items, total, nil
}

// CountByOrder возвращает число уведомлений по заказу.
// Используется тестом идемпотентности.
func (r *Repository) CountByOrder(ctx context.Context, orderID string) (int, error) {
	const query = `SELECT count(*) FROM notifications WHERE order_id = $1`

	var count int
	if err := r.pool.QueryRow(ctx, query, orderID).Scan(&count); err != nil {
		return 0, fmt.Errorf("посчитать уведомления заказа: %w", err)
	}
	return count, nil
}
