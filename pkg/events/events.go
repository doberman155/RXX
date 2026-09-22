// Package events описывает контракт сообщений Kafka.
// Schema Registry не используется: структуры событий живут здесь и
// разделяются всеми сервисами монорепозитория.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Топики. Ключ сообщения во всех топиках — orderId: это гарантирует,
// что события одного заказа попадают в одну партицию и обрабатываются по порядку.
const (
	TopicOrderCreated       = "order.created"
	TopicStockReserved      = "stock.reserved"
	TopicStockRejected      = "stock.rejected"
	TopicOrderCancelled     = "order.cancelled"
	TopicOrderStatusChanged = "order.status-changed"
)

// Типы событий совпадают с именами топиков — так проще сопоставлять их в логах.
const (
	TypeOrderCreated       = TopicOrderCreated
	TypeStockReserved      = TopicStockReserved
	TypeStockRejected      = TopicStockRejected
	TypeOrderCancelled     = TopicOrderCancelled
	TypeOrderStatusChanged = TopicOrderStatusChanged
)

// Currency — единственная поддерживаемая валюта учебного проекта.
const Currency = "RUB"

// Topics возвращает список всех бизнес-топиков (без DLQ).
func Topics() []string {
	return []string{
		TopicOrderCreated,
		TopicStockReserved,
		TopicStockRejected,
		TopicOrderCancelled,
		TopicOrderStatusChanged,
	}
}

// ErrInvalidEvent возвращается, когда сообщение не соответствует контракту.
var ErrInvalidEvent = errors.New("событие не соответствует контракту")

// Envelope — обязательная часть любого события.
type Envelope struct {
	EventID    string    `json:"event_id"`
	EventType  string    `json:"event_type"`
	OccurredAt time.Time `json:"occurred_at"`
	OrderID    string    `json:"order_id"`
}

// NewEnvelope формирует конверт нового события.
func NewEnvelope(eventType, orderID string) Envelope {
	return Envelope{
		EventID:    uuid.NewString(),
		EventType:  eventType,
		OccurredAt: time.Now().UTC(),
		OrderID:    orderID,
	}
}

// Validate проверяет обязательные поля конверта.
func (e Envelope) Validate(expectedType string) error {
	if e.EventID == "" {
		return fmt.Errorf("%w: пустой event_id", ErrInvalidEvent)
	}
	if _, err := uuid.Parse(e.EventID); err != nil {
		return fmt.Errorf("%w: event_id не является UUID", ErrInvalidEvent)
	}
	if e.OrderID == "" {
		return fmt.Errorf("%w: пустой order_id", ErrInvalidEvent)
	}
	if e.OccurredAt.IsZero() {
		return fmt.Errorf("%w: пустой occurred_at", ErrInvalidEvent)
	}
	if expectedType != "" && e.EventType != expectedType {
		return fmt.Errorf("%w: ожидался тип %q, получен %q", ErrInvalidEvent, expectedType, e.EventType)
	}
	return nil
}

// Item — позиция заказа в событии order.created.
type Item struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

// OrderCreated публикуется order-service после создания заказа.
type OrderCreated struct {
	Envelope
	UserID string `json:"user_id"`
	Items  []Item `json:"items"`
}

// Validate проверяет событие перед обработкой.
func (e OrderCreated) Validate() error {
	if err := e.Envelope.Validate(TypeOrderCreated); err != nil {
		return err
	}
	if e.UserID == "" {
		return fmt.Errorf("%w: пустой user_id", ErrInvalidEvent)
	}
	if len(e.Items) == 0 {
		return fmt.Errorf("%w: пустой список позиций", ErrInvalidEvent)
	}
	for i, it := range e.Items {
		if it.ProductID == "" {
			return fmt.Errorf("%w: items[%d].product_id пуст", ErrInvalidEvent, i)
		}
		if it.Quantity <= 0 {
			return fmt.Errorf("%w: items[%d].quantity должно быть больше нуля", ErrInvalidEvent, i)
		}
	}
	return nil
}

// ReservedItem — зарезервированная позиция с ценой на момент резерва.
type ReservedItem struct {
	ProductID      string `json:"product_id"`
	Name           string `json:"name"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

// StockReserved публикуется product-service при успешном резерве.
// Цены приезжают вместе с событием: order-service не ходит в чужую базу.
type StockReserved struct {
	Envelope
	Items            []ReservedItem `json:"items"`
	TotalAmountCents int64          `json:"total_amount_cents"`
	Currency         string         `json:"currency"`
}

// Validate проверяет событие перед обработкой.
func (e StockReserved) Validate() error {
	if err := e.Envelope.Validate(TypeStockReserved); err != nil {
		return err
	}
	if len(e.Items) == 0 {
		return fmt.Errorf("%w: пустой список зарезервированных позиций", ErrInvalidEvent)
	}
	if e.TotalAmountCents < 0 {
		return fmt.Errorf("%w: отрицательная сумма заказа", ErrInvalidEvent)
	}
	return nil
}

// Причины отказа в резерве.
const (
	ReasonInsufficientStock = "INSUFFICIENT_STOCK"
	ReasonProductNotFound   = "PRODUCT_NOT_FOUND"
)

// RejectedItem поясняет, какой позиции не хватило.
type RejectedItem struct {
	ProductID string `json:"product_id"`
	Requested int    `json:"requested"`
	Available int    `json:"available"`
}

// StockRejected публикуется product-service, если резерв невозможен.
type StockRejected struct {
	Envelope
	Reason  string         `json:"reason"`
	Message string         `json:"message"`
	Items   []RejectedItem `json:"items,omitempty"`
}

// Validate проверяет событие перед обработкой.
func (e StockRejected) Validate() error {
	if err := e.Envelope.Validate(TypeStockRejected); err != nil {
		return err
	}
	if e.Reason == "" {
		return fmt.Errorf("%w: пустая причина отказа", ErrInvalidEvent)
	}
	return nil
}

// OrderCancelled — компенсирующее событие: заказ отменён, резерв нужно вернуть.
type OrderCancelled struct {
	Envelope
	UserID string `json:"user_id"`
	// PreviousStatus — статус, из которого заказ был отменён.
	PreviousStatus string `json:"previous_status"`
	Reason         string `json:"reason"`
	// StockWasReserved — подсказка для product-service; он всё равно
	// перепроверяет наличие брони у себя.
	StockWasReserved bool `json:"stock_was_reserved"`
}

// Validate проверяет событие перед обработкой.
func (e OrderCancelled) Validate() error {
	if err := e.Envelope.Validate(TypeOrderCancelled); err != nil {
		return err
	}
	return nil
}

// OrderStatusChanged публикуется при каждом переходе статуса заказа.
type OrderStatusChanged struct {
	Envelope
	UserID           string `json:"user_id"`
	OldStatus        string `json:"old_status"`
	NewStatus        string `json:"new_status"`
	TotalAmountCents int64  `json:"total_amount_cents"`
	Reason           string `json:"reason,omitempty"`
}

// Validate проверяет событие перед обработкой.
func (e OrderStatusChanged) Validate() error {
	if err := e.Envelope.Validate(TypeOrderStatusChanged); err != nil {
		return err
	}
	if e.NewStatus == "" {
		return fmt.Errorf("%w: пустой new_status", ErrInvalidEvent)
	}
	return nil
}

// Validator — общий интерфейс событий, умеющих проверять себя.
type Validator interface {
	Validate() error
}

// Decode разбирает JSON-сообщение в событие и сразу проверяет контракт.
func Decode[T Validator](payload []byte) (T, error) {
	var evt T
	if err := json.Unmarshal(payload, &evt); err != nil {
		return evt, fmt.Errorf("%w: %s", ErrInvalidEvent, err.Error())
	}
	if err := evt.Validate(); err != nil {
		return evt, err
	}
	return evt, nil
}

// Encode сериализует событие в JSON.
func Encode(evt any) ([]byte, error) {
	payload, err := json.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("сериализовать событие: %w", err)
	}
	return payload, nil
}

// PeekEnvelope достаёт конверт, не разбирая тело события целиком.
// Нужен для логов и для записи в processed_events до полной валидации.
func PeekEnvelope(payload []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return env, fmt.Errorf("%w: %s", ErrInvalidEvent, err.Error())
	}
	return env, nil
}
