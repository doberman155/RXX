// Package order реализует order-service: создание заказов, явные переходы
// статусов и оркестрацию саги резервирования на событиях Kafka.
package order

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/hshsb/shop/pkg/auth"
)

// Status — статус заказа.
type Status string

// Допустимые статусы заказа.
const (
	StatusNew       Status = "NEW"
	StatusReserved  Status = "RESERVED"
	StatusCompleted Status = "COMPLETED"
	StatusCancelled Status = "CANCELLED"
)

// String реализует fmt.Stringer.
func (s Status) String() string { return string(s) }

// Valid сообщает, известен ли статус.
func (s Status) Valid() bool {
	switch s {
	case StatusNew, StatusReserved, StatusCompleted, StatusCancelled:
		return true
	default:
		return false
	}
}

// Terminal сообщает, что заказ больше не меняет статус.
func (s Status) Terminal() bool { return s == StatusCompleted || s == StatusCancelled }

// allowedTransitions описывает граф переходов явно: всё, чего здесь нет,
// отклоняется.
//
//	NEW      -> RESERVED | CANCELLED
//	RESERVED -> COMPLETED | CANCELLED (с компенсацией остатка)
var allowedTransitions = map[Status][]Status{
	StatusNew:       {StatusReserved, StatusCancelled},
	StatusReserved:  {StatusCompleted, StatusCancelled},
	StatusCompleted: {},
	StatusCancelled: {},
}

// CanTransitionTo проверяет допустимость перехода.
func (s Status) CanTransitionTo(next Status) bool {
	for _, allowed := range allowedTransitions[s] {
		if allowed == next {
			return true
		}
	}
	return false
}

// ErrInvalidTransition — попытка недопустимого перехода статуса.
type ErrInvalidTransition struct {
	From Status
	To   Status
}

// Error реализует интерфейс error.
func (e ErrInvalidTransition) Error() string {
	return fmt.Sprintf("недопустимый переход статуса: %s -> %s", e.From, e.To)
}

// Ошибки предметной области.
var (
	ErrNotFound              = errors.New("заказ не найден")
	ErrForbidden             = errors.New("нет доступа к заказу")
	ErrIdempotencyKeyReused  = errors.New("этот Idempotency-Key уже использован с другим телом запроса")
	ErrIdempotencyKeyMissing = errors.New("требуется заголовок Idempotency-Key")
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

// Item — позиция заказа. Цена и название приезжают из события stock.reserved.
type Item struct {
	ID             uuid.UUID
	ProductID      uuid.UUID
	ProductName    string
	Quantity       int
	UnitPriceCents int64
}

// Order — заказ пользователя.
type Order struct {
	ID               uuid.UUID
	UserID           uuid.UUID
	Status           Status
	TotalAmountCents int64
	Currency         string
	CancelReason     string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	Items            []Item
}

// Actor — тот, кто выполняет операцию: обычный пользователь видит только
// свои заказы, ADMIN — все.
type Actor struct {
	UserID uuid.UUID
	Role   auth.Role
}

// CanAccess сообщает, вправе ли actor работать с заказом.
func (a Actor) CanAccess(o Order) bool {
	return a.Role == auth.RoleAdmin || a.UserID == o.UserID
}

// CreateItem — позиция в запросе на создание заказа.
type CreateItem struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

// CreateInput — данные для создания заказа.
type CreateInput struct {
	Actor          Actor
	Items          []CreateItem
	IdempotencyKey string
}

// Ограничения на заказ.
const (
	MaxItemsPerOrder    = 50
	MaxQuantityPerItem  = 1000
	MaxIdempotencyKeyLn = 255
)

// Validate проверяет запрос и возвращает позиции, сложенные по товарам.
func (in CreateInput) Validate() ([]CreateItem, error) {
	key := strings.TrimSpace(in.IdempotencyKey)
	if key == "" {
		return nil, ErrIdempotencyKeyMissing
	}
	if len(key) > MaxIdempotencyKeyLn {
		return nil, invalid("Idempotency-Key", "ключ идемпотентности длиннее %d символов", MaxIdempotencyKeyLn)
	}
	if len(in.Items) == 0 {
		return nil, invalid("items", "заказ должен содержать хотя бы одну позицию")
	}
	if len(in.Items) > MaxItemsPerOrder {
		return nil, invalid("items", "в заказе больше %d позиций", MaxItemsPerOrder)
	}

	merged := make(map[uuid.UUID]int, len(in.Items))
	order := make([]uuid.UUID, 0, len(in.Items))

	for i, it := range in.Items {
		id, err := uuid.Parse(strings.TrimSpace(it.ProductID))
		if err != nil {
			return nil, invalid("items", "items[%d].product_id должен быть UUID", i)
		}
		if it.Quantity <= 0 {
			return nil, invalid("items", "items[%d].quantity должно быть больше нуля", i)
		}
		if it.Quantity > MaxQuantityPerItem {
			return nil, invalid("items", "items[%d].quantity больше %d", i, MaxQuantityPerItem)
		}
		if _, seen := merged[id]; !seen {
			order = append(order, id)
		}
		merged[id] += it.Quantity
	}

	items := make([]CreateItem, 0, len(order))
	for _, id := range order {
		qty := merged[id]
		if qty > MaxQuantityPerItem {
			return nil, invalid("items", "суммарное количество товара %s больше %d", id, MaxQuantityPerItem)
		}
		items = append(items, CreateItem{ProductID: id.String(), Quantity: qty})
	}
	return items, nil
}

// RequestHash — отпечаток содержимого запроса. Нужен, чтобы один и тот же
// Idempotency-Key нельзя было переиспользовать с другим составом заказа.
func RequestHash(items []CreateItem) string {
	normalized := make([]string, 0, len(items))
	for _, it := range items {
		normalized = append(normalized, fmt.Sprintf("%s:%d", strings.ToLower(it.ProductID), it.Quantity))
	}
	sort.Strings(normalized)

	sum := sha256.Sum256([]byte(strings.Join(normalized, "|")))
	return hex.EncodeToString(sum[:])
}
