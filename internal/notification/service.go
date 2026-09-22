package notification

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/hshsb/shop/pkg/events"
	"github.com/hshsb/shop/pkg/idempotency"
	"github.com/hshsb/shop/pkg/logger"
	"github.com/hshsb/shop/pkg/postgres"
)

// Service — бизнес-логика notification-service.
type Service struct {
	repo          *Repository
	consumerGroup string
	log           *slog.Logger
}

// NewService собирает сервис.
func NewService(repo *Repository, consumerGroup string, log *slog.Logger) *Service {
	return &Service{repo: repo, consumerGroup: consumerGroup, log: log}
}

// List возвращает сохранённые уведомления.
func (s *Service) List(ctx context.Context, f ListFilter) ([]Notification, int, error) {
	return s.repo.List(ctx, f)
}

// HandleOrderCreated сохраняет уведомление о создании заказа.
func (s *Service) HandleOrderCreated(ctx context.Context, evt events.OrderCreated) error {
	orderID, userID, err := parseIDs(evt.OrderID, evt.UserID)
	if err != nil {
		return err
	}
	eventID, err := uuid.Parse(evt.EventID)
	if err != nil {
		return fmt.Errorf("event_id %q не является UUID", evt.EventID)
	}

	return s.save(ctx, events.TopicOrderCreated, evt.EventID, Notification{
		ID:        uuid.New(),
		OrderID:   orderID,
		UserID:    userID,
		EventID:   eventID,
		EventType: evt.EventType,
		Kind:      KindOrderCreated,
		NewStatus: "NEW",
		Message:   createdMessage(evt.OrderID, len(evt.Items)),
	})
}

// HandleOrderStatusChanged сохраняет уведомление о смене статуса заказа.
func (s *Service) HandleOrderStatusChanged(ctx context.Context, evt events.OrderStatusChanged) error {
	orderID, userID, err := parseIDs(evt.OrderID, evt.UserID)
	if err != nil {
		return err
	}
	eventID, err := uuid.Parse(evt.EventID)
	if err != nil {
		return fmt.Errorf("event_id %q не является UUID", evt.EventID)
	}

	return s.save(ctx, events.TopicOrderStatusChanged, evt.EventID, Notification{
		ID:        uuid.New(),
		OrderID:   orderID,
		UserID:    userID,
		EventID:   eventID,
		EventType: evt.EventType,
		Kind:      KindOrderStatusChanged,
		OldStatus: evt.OldStatus,
		NewStatus: evt.NewStatus,
		Message:   statusMessage(evt.OrderID, evt.OldStatus, evt.NewStatus, evt.Reason),
	})
}

// save записывает уведомление и отметку об обработке события в одной
// транзакции: повторная доставка не создаёт второе уведомление.
func (s *Service) save(ctx context.Context, topic, eventID string, n Notification) error {
	log := logger.WithOrderID(logger.From(ctx), n.OrderID.String())

	return postgres.InTx(ctx, s.repo.Pool(), func(ctx context.Context, tx pgx.Tx) error {
		first, err := idempotency.MarkProcessed(ctx, tx, s.consumerGroup, eventID, topic)
		if err != nil {
			return err
		}
		if !first {
			log.Info("событие уже обработано, уведомление не дублируется",
				slog.String(logger.KeyEventID, eventID),
				slog.String(logger.KeyTopic, topic),
			)
			return nil
		}

		inserted, err := s.repo.Insert(ctx, tx, n)
		if err != nil {
			return err
		}
		if !inserted {
			log.Warn("уведомление по этому событию уже существует",
				slog.String(logger.KeyEventID, eventID))
			return nil
		}

		// Требование ТЗ: уведомление дублируется в лог.
		log.Info("уведомление сохранено",
			slog.String(logger.KeyEventID, eventID),
			slog.String("kind", n.Kind),
			slog.String("new_status", n.NewStatus),
			slog.String("message", n.Message),
		)
		return nil
	})
}

func parseIDs(orderID, userID string) (uuid.UUID, uuid.UUID, error) {
	oid, err := uuid.Parse(orderID)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("order_id %q не является UUID", orderID)
	}
	uid, err := uuid.Parse(userID)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("user_id %q не является UUID", userID)
	}
	return oid, uid, nil
}
