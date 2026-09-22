package order

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/hshsb/shop/pkg/events"
	"github.com/hshsb/shop/pkg/postgres"
)

// Repository — доступ к таблицам orders, order_items и idempotency_keys.
type Repository struct {
	pool *postgres.Pool
}

// NewRepository создаёт репозиторий.
func NewRepository(pool *postgres.Pool) *Repository { return &Repository{pool: pool} }

// Pool возвращает пул: нужен сервису для транзакций с outbox.
func (r *Repository) Pool() *postgres.Pool { return r.pool }

// Ping используется readiness-пробой.
func (r *Repository) Ping(ctx context.Context) error { return r.pool.Ping(ctx) }

const orderColumns = `id, user_id, status, total_amount_cents, currency, coalesce(cancel_reason, ''), created_at, updated_at`

// Insert сохраняет заказ вместе с позициями внутри переданной транзакции.
func (r *Repository) Insert(ctx context.Context, tx pgx.Tx, o Order) error {
	const insertOrder = `
		INSERT INTO orders (id, user_id, status, total_amount_cents, currency)
		VALUES ($1, $2, $3, $4, $5)`

	if _, err := tx.Exec(ctx, insertOrder, o.ID, o.UserID, o.Status, o.TotalAmountCents, o.Currency); err != nil {
		return fmt.Errorf("создать заказ: %w", err)
	}

	const insertItem = `
		INSERT INTO order_items (id, order_id, product_id, product_name, quantity, unit_price_cents)
		VALUES ($1, $2, $3, $4, $5, $6)`

	for _, item := range o.Items {
		if _, err := tx.Exec(ctx, insertItem, item.ID, o.ID, item.ProductID, item.ProductName,
			item.Quantity, item.UnitPriceCents); err != nil {
			return fmt.Errorf("сохранить позицию заказа: %w", err)
		}
	}
	return nil
}

// GetByID возвращает заказ с позициями.
func (r *Repository) GetByID(ctx context.Context, id uuid.UUID) (Order, error) {
	const query = `SELECT ` + orderColumns + ` FROM orders WHERE id = $1`

	o, err := scanOrder(r.pool.QueryRow(ctx, query, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Order{}, ErrNotFound
		}
		return Order{}, fmt.Errorf("получить заказ: %w", err)
	}

	items, err := loadItems(ctx, r.pool, id)
	if err != nil {
		return Order{}, err
	}
	o.Items = items
	return o, nil
}

// LockByID читает заказ с блокировкой строки: нужен перед сменой статуса,
// чтобы параллельные обработчики не выполнили конфликтующие переходы.
func (r *Repository) LockByID(ctx context.Context, tx pgx.Tx, id uuid.UUID) (Order, error) {
	const query = `SELECT ` + orderColumns + ` FROM orders WHERE id = $1 FOR UPDATE`

	o, err := scanOrder(tx.QueryRow(ctx, query, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Order{}, ErrNotFound
		}
		return Order{}, fmt.Errorf("заблокировать заказ: %w", err)
	}
	return o, nil
}

// ListFilter — фильтр списка заказов.
type ListFilter struct {
	// UserID != nil ограничивает выборку заказами пользователя.
	UserID *uuid.UUID
	Status Status
	Limit  int
	Offset int
}

// List возвращает страницу заказов вместе с позициями.
func (r *Repository) List(ctx context.Context, f ListFilter) ([]Order, int, error) {
	const query = `
		SELECT ` + orderColumns + `, count(*) OVER () AS total
		FROM orders
		WHERE ($1::uuid IS NULL OR user_id = $1)
		  AND ($2 = '' OR status = $2)
		ORDER BY created_at DESC, id
		LIMIT $3 OFFSET $4`

	rows, err := r.pool.Query(ctx, query, f.UserID, string(f.Status), f.Limit, f.Offset)
	if err != nil {
		return nil, 0, fmt.Errorf("получить список заказов: %w", err)
	}
	defer rows.Close()

	var (
		orders []Order
		total  int
	)
	for rows.Next() {
		var o Order
		if err := rows.Scan(&o.ID, &o.UserID, &o.Status, &o.TotalAmountCents, &o.Currency,
			&o.CancelReason, &o.CreatedAt, &o.UpdatedAt, &total); err != nil {
			return nil, 0, fmt.Errorf("прочитать заказ: %w", err)
		}
		orders = append(orders, o)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("обойти список заказов: %w", err)
	}

	for i := range orders {
		items, err := loadItems(ctx, r.pool, orders[i].ID)
		if err != nil {
			return nil, 0, err
		}
		orders[i].Items = items
	}
	return orders, total, nil
}

// UpdateStatus меняет статус заказа и, при необходимости, сумму и причину отмены.
func (r *Repository) UpdateStatus(ctx context.Context, tx pgx.Tx, id uuid.UUID, status Status, totalCents int64, cancelReason string) error {
	const query = `
		UPDATE orders
		SET status = $2,
		    total_amount_cents = $3,
		    cancel_reason = NULLIF($4, ''),
		    updated_at = now()
		WHERE id = $1`

	tag, err := tx.Exec(ctx, query, id, status, totalCents, cancelReason)
	if err != nil {
		return fmt.Errorf("обновить статус заказа: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ApplyReservedPrices проставляет цены и названия товаров, пришедшие в
// событии stock.reserved: собственных данных о каталоге у сервиса нет.
func (r *Repository) ApplyReservedPrices(ctx context.Context, tx pgx.Tx, orderID uuid.UUID, items []events.ReservedItem) error {
	const query = `
		UPDATE order_items
		SET unit_price_cents = $3, product_name = $4
		WHERE order_id = $1 AND product_id = $2`

	for _, item := range items {
		productID, err := uuid.Parse(item.ProductID)
		if err != nil {
			return fmt.Errorf("product_id %q не является UUID", item.ProductID)
		}
		if _, err := tx.Exec(ctx, query, orderID, productID, item.UnitPriceCents, item.Name); err != nil {
			return fmt.Errorf("обновить цену позиции заказа: %w", err)
		}
	}
	return nil
}

// FindIdempotentOrder ищет заказ, созданный ранее с тем же ключом идемпотентности.
func (r *Repository) FindIdempotentOrder(ctx context.Context, tx pgx.Tx, userID uuid.UUID, key string) (uuid.UUID, string, bool, error) {
	const query = `SELECT order_id, request_hash FROM idempotency_keys WHERE user_id = $1 AND key = $2`

	var (
		orderID uuid.UUID
		hash    string
	)
	err := tx.QueryRow(ctx, query, userID, key).Scan(&orderID, &hash)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return uuid.Nil, "", false, nil
	case err != nil:
		return uuid.Nil, "", false, fmt.Errorf("проверить ключ идемпотентности: %w", err)
	default:
		return orderID, hash, true, nil
	}
}

// SaveIdempotencyKey связывает ключ идемпотентности с созданным заказом.
func (r *Repository) SaveIdempotencyKey(ctx context.Context, tx pgx.Tx, userID uuid.UUID, key, hash string, orderID uuid.UUID) error {
	const query = `
		INSERT INTO idempotency_keys (key, user_id, request_hash, order_id)
		VALUES ($1, $2, $3, $4)`

	if _, err := tx.Exec(ctx, query, key, userID, hash, orderID); err != nil {
		if postgres.IsUniqueViolation(err) {
			// Параллельный запрос с тем же ключом успел раньше.
			return ErrDuplicateKey
		}
		return fmt.Errorf("сохранить ключ идемпотентности: %w", err)
	}
	return nil
}

// ErrDuplicateKey сигнализирует о гонке двух запросов с одним Idempotency-Key.
var ErrDuplicateKey = errors.New("ключ идемпотентности уже сохранён")

func loadItems(ctx context.Context, q querier, orderID uuid.UUID) ([]Item, error) {
	const query = `
		SELECT id, product_id, product_name, quantity, unit_price_cents
		FROM order_items
		WHERE order_id = $1
		ORDER BY product_id`

	rows, err := q.Query(ctx, query, orderID)
	if err != nil {
		return nil, fmt.Errorf("получить позиции заказа: %w", err)
	}
	defer rows.Close()

	var items []Item
	for rows.Next() {
		var it Item
		if err := rows.Scan(&it.ID, &it.ProductID, &it.ProductName, &it.Quantity, &it.UnitPriceCents); err != nil {
			return nil, fmt.Errorf("прочитать позицию заказа: %w", err)
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("обойти позиции заказа: %w", err)
	}
	return items, nil
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func scanOrder(row pgx.Row) (Order, error) {
	var o Order
	if err := row.Scan(&o.ID, &o.UserID, &o.Status, &o.TotalAmountCents, &o.Currency,
		&o.CancelReason, &o.CreatedAt, &o.UpdatedAt); err != nil {
		return Order{}, err
	}
	return o, nil
}
