// Package product реализует product-service: каталог товаров, остатки и
// резервирование под заказ по событиям от order-service.
package product

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/hshsb/shop/pkg/events"
)

// Ошибки предметной области.
var (
	ErrNotFound       = errors.New("товар не найден")
	ErrSKUAlreadyUsed = errors.New("товар с таким SKU уже существует")
)

// ValidationError — ошибка входных данных; REST-слой отдаёт её как 400.
type ValidationError struct {
	Field   string
	Message string
}

// Error реализует интерфейс error.
func (e ValidationError) Error() string { return e.Message }

func invalid(field, format string, args ...any) error {
	return ValidationError{Field: field, Message: fmt.Sprintf(format, args...)}
}

// Статусы брони.
const (
	ReservationHeld     = "HELD"
	ReservationReleased = "RELEASED"
)

// Product — позиция каталога вместе с остатком на складе.
type Product struct {
	ID          uuid.UUID
	SKU         string
	Name        string
	Description string
	Category    string
	PriceCents  int64
	Stock       int
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// CreateInput — данные для создания товара.
type CreateInput struct {
	SKU         string
	Name        string
	Description string
	Category    string
	PriceCents  int64
	Stock       int
}

// Validate проверяет входные данные создания.
func (in CreateInput) Validate() error {
	if err := validateText("sku", in.SKU, 64); err != nil {
		return err
	}
	if err := validateText("name", in.Name, 255); err != nil {
		return err
	}
	if err := validateText("category", in.Category, 64); err != nil {
		return err
	}
	if len(in.Description) > 2000 {
		return invalid("description", "описание длиннее 2000 символов")
	}
	if in.PriceCents < 0 {
		return invalid("price_cents", "цена не может быть отрицательной")
	}
	if in.Stock < 0 {
		return invalid("stock", "остаток не может быть отрицательным")
	}
	return nil
}

// UpdateInput — данные для полного обновления товара.
type UpdateInput struct {
	Name        string
	Description string
	Category    string
	PriceCents  int64
	Stock       int
}

// Validate проверяет входные данные обновления.
func (in UpdateInput) Validate() error {
	return CreateInput{
		SKU:         "placeholder",
		Name:        in.Name,
		Description: in.Description,
		Category:    in.Category,
		PriceCents:  in.PriceCents,
		Stock:       in.Stock,
	}.Validate()
}

func validateText(field, value string, maxLen int) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return invalid(field, "поле %s обязательно", field)
	}
	if len(value) > maxLen {
		return invalid(field, "поле %s длиннее %d символов", field, maxLen)
	}
	return nil
}

// ListFilter — фильтр каталога: категория и постраничный вывод.
type ListFilter struct {
	Category string
	Limit    int
	Offset   int
}

// ReserveOutcome — результат попытки зарезервировать позиции заказа.
type ReserveOutcome struct {
	// Reserved=true означает успешный резерв всех позиций.
	Reserved   bool
	Items      []events.ReservedItem
	TotalCents int64
	Reason     string
	Message    string
	Rejected   []events.RejectedItem
}

// ReleasedItem — возвращённая на склад позиция.
type ReleasedItem struct {
	ProductID uuid.UUID
	Quantity  int
}

// aggregateItems складывает количества по одинаковым товарам: дубликаты в
// событии не должны ломать уникальный индекс броней.
func aggregateItems(items []events.Item) ([]events.Item, error) {
	merged := make(map[uuid.UUID]int, len(items))
	order := make([]uuid.UUID, 0, len(items))

	for _, it := range items {
		id, err := uuid.Parse(it.ProductID)
		if err != nil {
			return nil, fmt.Errorf("product_id %q не является UUID", it.ProductID)
		}
		if _, seen := merged[id]; !seen {
			order = append(order, id)
		}
		merged[id] += it.Quantity
	}

	out := make([]events.Item, 0, len(order))
	for _, id := range order {
		out = append(out, events.Item{ProductID: id.String(), Quantity: merged[id]})
	}
	return out, nil
}
