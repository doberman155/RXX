package product

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/hshsb/shop/pkg/events"
	"github.com/hshsb/shop/pkg/postgres"
)

// Repository — доступ к таблицам products и reservations.
type Repository struct {
	pool *postgres.Pool
}

// NewRepository создаёт репозиторий.
func NewRepository(pool *postgres.Pool) *Repository { return &Repository{pool: pool} }

// Pool возвращает пул: нужен сервису для транзакций с outbox.
func (r *Repository) Pool() *postgres.Pool { return r.pool }

// Ping используется readiness-пробой.
func (r *Repository) Ping(ctx context.Context) error { return r.pool.Ping(ctx) }

const productColumns = `id, sku, name, description, category, price_cents, stock, created_at, updated_at`

// Create добавляет товар в каталог.
func (r *Repository) Create(ctx context.Context, p Product) (Product, error) {
	const query = `
		INSERT INTO products (id, sku, name, description, category, price_cents, stock)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING ` + productColumns

	created, err := scanProduct(r.pool.QueryRow(ctx, query,
		p.ID, p.SKU, p.Name, p.Description, p.Category, p.PriceCents, p.Stock))
	if err != nil {
		if postgres.IsUniqueViolation(err) {
			return Product{}, ErrSKUAlreadyUsed
		}
		return Product{}, fmt.Errorf("создать товар: %w", err)
	}
	return created, nil
}

// GetByID возвращает живой (не удалённый) товар.
func (r *Repository) GetByID(ctx context.Context, id uuid.UUID) (Product, error) {
	const query = `SELECT ` + productColumns + ` FROM products WHERE id = $1 AND deleted_at IS NULL`

	p, err := scanProduct(r.pool.QueryRow(ctx, query, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Product{}, ErrNotFound
		}
		return Product{}, fmt.Errorf("получить товар: %w", err)
	}
	return p, nil
}

// Update полностью обновляет карточку товара.
func (r *Repository) Update(ctx context.Context, id uuid.UUID, in UpdateInput) (Product, error) {
	const query = `
		UPDATE products
		SET name = $2, description = $3, category = $4, price_cents = $5, stock = $6, updated_at = now()
		WHERE id = $1 AND deleted_at IS NULL
		RETURNING ` + productColumns

	p, err := scanProduct(r.pool.QueryRow(ctx, query,
		id, in.Name, in.Description, in.Category, in.PriceCents, in.Stock))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Product{}, ErrNotFound
		}
		return Product{}, fmt.Errorf("обновить товар: %w", err)
	}
	return p, nil
}

// SoftDelete помечает товар удалённым. Физическое удаление не используется:
// на товар могут ссылаться существующие брони.
func (r *Repository) SoftDelete(ctx context.Context, id uuid.UUID) error {
	const query = `UPDATE products SET deleted_at = now(), updated_at = now() WHERE id = $1 AND deleted_at IS NULL`

	tag, err := r.pool.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("удалить товар: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// List возвращает страницу каталога с фильтром по категории.
func (r *Repository) List(ctx context.Context, f ListFilter) ([]Product, int, error) {
	const query = `
		SELECT ` + productColumns + `, count(*) OVER () AS total
		FROM products
		WHERE deleted_at IS NULL
		  AND ($1 = '' OR lower(category) = lower($1))
		ORDER BY created_at DESC, id
		LIMIT $2 OFFSET $3`

	rows, err := r.pool.Query(ctx, query, strings.TrimSpace(f.Category), f.Limit, f.Offset)
	if err != nil {
		return nil, 0, fmt.Errorf("получить каталог: %w", err)
	}
	defer rows.Close()

	var (
		items []Product
		total int
	)
	for rows.Next() {
		var p Product
		if err := rows.Scan(&p.ID, &p.SKU, &p.Name, &p.Description, &p.Category,
			&p.PriceCents, &p.Stock, &p.CreatedAt, &p.UpdatedAt, &total); err != nil {
			return nil, 0, fmt.Errorf("прочитать товар: %w", err)
		}
		items = append(items, p)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("обойти каталог: %w", err)
	}
	return items, total, nil
}

// lockedProduct — строка товара, заблокированная на время резерва.
type lockedProduct struct {
	id         uuid.UUID
	name       string
	priceCents int64
	stock      int
}

// Reserve пытается зарезервировать позиции заказа внутри переданной транзакции.
// Строки блокируются в порядке возрастания id — это исключает взаимные блокировки
// при параллельной обработке заказов с пересекающимися товарами.
func (r *Repository) Reserve(ctx context.Context, tx pgx.Tx, orderID uuid.UUID, items []events.Item) (ReserveOutcome, error) {
	ids := make([]uuid.UUID, 0, len(items))
	requested := make(map[uuid.UUID]int, len(items))
	for _, it := range items {
		id, err := uuid.Parse(it.ProductID)
		if err != nil {
			return ReserveOutcome{}, fmt.Errorf("product_id %q не является UUID", it.ProductID)
		}
		ids = append(ids, id)
		requested[id] = it.Quantity
	}

	locked, err := lockProducts(ctx, tx, ids)
	if err != nil {
		return ReserveOutcome{}, err
	}

	// Проверяем доступность всех позиций до первого изменения остатка:
	// резерв должен быть атомарным «всё или ничего».
	var rejected []events.RejectedItem
	for _, it := range items {
		id := uuid.MustParse(it.ProductID)
		row, ok := locked[id]
		if !ok {
			rejected = append(rejected, events.RejectedItem{ProductID: it.ProductID, Requested: it.Quantity, Available: 0})
			continue
		}
		if row.stock < it.Quantity {
			rejected = append(rejected, events.RejectedItem{ProductID: it.ProductID, Requested: it.Quantity, Available: row.stock})
		}
	}

	if len(rejected) > 0 {
		return buildRejection(locked, rejected), nil
	}

	reserved := make([]events.ReservedItem, 0, len(items))
	var total int64

	for _, it := range items {
		id := uuid.MustParse(it.ProductID)
		row := locked[id]

		if err := decreaseStock(ctx, tx, id, it.Quantity); err != nil {
			return ReserveOutcome{}, err
		}
		if err := insertReservation(ctx, tx, orderID, id, it.Quantity); err != nil {
			return ReserveOutcome{}, err
		}

		reserved = append(reserved, events.ReservedItem{
			ProductID:      id.String(),
			Name:           row.name,
			Quantity:       it.Quantity,
			UnitPriceCents: row.priceCents,
		})
		total += row.priceCents * int64(it.Quantity)
	}

	return ReserveOutcome{Reserved: true, Items: reserved, TotalCents: total}, nil
}

func buildRejection(locked map[uuid.UUID]lockedProduct, rejected []events.RejectedItem) ReserveOutcome {
	reason := events.ReasonInsufficientStock
	for _, it := range rejected {
		if _, ok := locked[uuid.MustParse(it.ProductID)]; !ok {
			reason = events.ReasonProductNotFound
			break
		}
	}

	message := "недостаточно товара на складе"
	if reason == events.ReasonProductNotFound {
		message = "товар отсутствует в каталоге"
	}

	return ReserveOutcome{Reason: reason, Message: message, Rejected: rejected}
}

func lockProducts(ctx context.Context, tx pgx.Tx, ids []uuid.UUID) (map[uuid.UUID]lockedProduct, error) {
	const query = `
		SELECT id, name, price_cents, stock
		FROM products
		WHERE id = ANY($1) AND deleted_at IS NULL
		ORDER BY id
		FOR UPDATE`

	rows, err := tx.Query(ctx, query, ids)
	if err != nil {
		return nil, fmt.Errorf("заблокировать товары для резерва: %w", err)
	}
	defer rows.Close()

	locked := make(map[uuid.UUID]lockedProduct, len(ids))
	for rows.Next() {
		var p lockedProduct
		if err := rows.Scan(&p.id, &p.name, &p.priceCents, &p.stock); err != nil {
			return nil, fmt.Errorf("прочитать товар для резерва: %w", err)
		}
		locked[p.id] = p
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("обойти товары для резерва: %w", err)
	}
	return locked, nil
}

func decreaseStock(ctx context.Context, tx pgx.Tx, id uuid.UUID, qty int) error {
	const query = `UPDATE products SET stock = stock - $2, updated_at = now() WHERE id = $1 AND stock >= $2`

	tag, err := tx.Exec(ctx, query, id, qty)
	if err != nil {
		return fmt.Errorf("списать остаток товара %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		// Строка заблокирована выше, поэтому сюда попасть нельзя;
		// проверка защищает от логической ошибки в коде.
		return fmt.Errorf("остаток товара %s изменился во время резерва", id)
	}
	return nil
}

func insertReservation(ctx context.Context, tx pgx.Tx, orderID, productID uuid.UUID, qty int) error {
	const query = `
		INSERT INTO reservations (id, order_id, product_id, quantity, status)
		VALUES ($1, $2, $3, $4, 'HELD')`

	if _, err := tx.Exec(ctx, query, uuid.New(), orderID, productID, qty); err != nil {
		return fmt.Errorf("сохранить бронь заказа %s: %w", orderID, err)
	}
	return nil
}

// Release возвращает остатки по заказу и помечает брони освобождёнными.
// Если активных броней нет, возвращается пустой список — повторный вызов
// безопасен.
func (r *Repository) Release(ctx context.Context, tx pgx.Tx, orderID uuid.UUID) ([]ReleasedItem, error) {
	const selectQuery = `
		SELECT id, product_id, quantity
		FROM reservations
		WHERE order_id = $1 AND status = 'HELD'
		ORDER BY product_id
		FOR UPDATE`

	rows, err := tx.Query(ctx, selectQuery, orderID)
	if err != nil {
		return nil, fmt.Errorf("выбрать брони заказа %s: %w", orderID, err)
	}

	type held struct {
		id        uuid.UUID
		productID uuid.UUID
		quantity  int
	}

	var reservations []held
	for rows.Next() {
		var h held
		if err := rows.Scan(&h.id, &h.productID, &h.quantity); err != nil {
			rows.Close()
			return nil, fmt.Errorf("прочитать бронь заказа %s: %w", orderID, err)
		}
		reservations = append(reservations, h)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("обойти брони заказа %s: %w", orderID, err)
	}

	released := make([]ReleasedItem, 0, len(reservations))
	for _, h := range reservations {
		const restore = `UPDATE products SET stock = stock + $2, updated_at = now() WHERE id = $1`
		if _, err := tx.Exec(ctx, restore, h.productID, h.quantity); err != nil {
			return nil, fmt.Errorf("вернуть остаток товара %s: %w", h.productID, err)
		}

		const markReleased = `UPDATE reservations SET status = 'RELEASED', released_at = now() WHERE id = $1`
		if _, err := tx.Exec(ctx, markReleased, h.id); err != nil {
			return nil, fmt.Errorf("пометить бронь %s освобождённой: %w", h.id, err)
		}

		released = append(released, ReleasedItem{ProductID: h.productID, Quantity: h.quantity})
	}

	return released, nil
}

// HasReservation сообщает, есть ли по заказу хотя бы одна бронь
// (в любом статусе). Используется как защита от повторного резерва.
func (r *Repository) HasReservation(ctx context.Context, tx pgx.Tx, orderID uuid.UUID) (bool, error) {
	const query = `SELECT EXISTS (SELECT 1 FROM reservations WHERE order_id = $1)`

	var exists bool
	if err := tx.QueryRow(ctx, query, orderID).Scan(&exists); err != nil {
		return false, fmt.Errorf("проверить брони заказа %s: %w", orderID, err)
	}
	return exists, nil
}

func scanProduct(row pgx.Row) (Product, error) {
	var p Product
	if err := row.Scan(&p.ID, &p.SKU, &p.Name, &p.Description, &p.Category,
		&p.PriceCents, &p.Stock, &p.CreatedAt, &p.UpdatedAt); err != nil {
		return Product{}, err
	}
	return p, nil
}
