// Package notification реализует notification-service: слушает события
// заказа и сохраняет уведомления. Реальная отправка письма в объём работ
// не входит — уведомление пишется в таблицу и дублируется в лог.
package notification

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ErrNotFound возвращается, когда уведомление не найдено.
var ErrNotFound = errors.New("уведомление не найдено")

// Виды уведомлений.
const (
	KindOrderCreated       = "ORDER_CREATED"
	KindOrderStatusChanged = "ORDER_STATUS_CHANGED"
)

// Notification — сохранённое уведомление.
type Notification struct {
	ID        uuid.UUID
	OrderID   uuid.UUID
	UserID    uuid.UUID
	EventID   uuid.UUID
	EventType string
	Kind      string
	OldStatus string
	NewStatus string
	Message   string
	CreatedAt time.Time
}

// ListFilter — фильтр списка уведомлений.
type ListFilter struct {
	OrderID *uuid.UUID
	UserID  *uuid.UUID
	Limit   int
	Offset  int
}

// createdMessage формирует текст уведомления о создании заказа.
func createdMessage(orderID string, items int) string {
	return fmt.Sprintf("Заказ %s создан, позиций: %d. Ожидается резерв товара.", orderID, items)
}

// statusMessage формирует текст уведомления о смене статуса.
func statusMessage(orderID, oldStatus, newStatus, reason string) string {
	base := fmt.Sprintf("Заказ %s: статус изменён с %s на %s.", orderID, oldStatus, newStatus)
	if reason != "" {
		base += " Причина: " + reason + "."
	}
	return base
}
