package order

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/hshsb/shop/pkg/auth"
	"github.com/hshsb/shop/pkg/events"
	"github.com/hshsb/shop/pkg/idempotency"
	"github.com/hshsb/shop/pkg/logger"
	"github.com/hshsb/shop/pkg/outbox"
	"github.com/hshsb/shop/pkg/postgres"
)

// Причины смены статуса, попадающие в события и в поле cancel_reason.
const (
	ReasonStockRejected   = "отказано в резерве товара"
	ReasonCancelledByUser = "отменён пользователем"
)

// Service — бизнес-логика order-service и оркестратор саги.
type Service struct {
	repo          *Repository
	consumerGroup string
	// autoComplete переводит заказ RESERVED -> COMPLETED сразу после
	// успешного резерва, чтобы полный цикл проходил без ручных действий.
	autoComplete bool
	log          *slog.Logger
}

// NewService собирает сервис.
func NewService(repo *Repository, consumerGroup string, autoComplete bool, log *slog.Logger) *Service {
	return &Service{repo: repo, consumerGroup: consumerGroup, autoComplete: autoComplete, log: log}
}

// Create создаёт заказ в статусе NEW и публикует order.created через outbox.
// Повторный вызов с тем же Idempotency-Key возвращает существующий заказ
// (второй результат — true).
func (s *Service) Create(ctx context.Context, in CreateInput) (Order, bool, error) {
	items, err := in.Validate()
	if err != nil {
		return Order{}, false, err
	}

	key := strings.TrimSpace(in.IdempotencyKey)
	hash := RequestHash(items)

	created, existing, err := s.create(ctx, in.Actor, key, hash, items)
	if errors.Is(err, ErrDuplicateKey) {
		// Гонка двух одинаковых запросов: победитель уже сохранил заказ,
		// поэтому повторяем попытку и возвращаем его результат.
		created, existing, err = s.create(ctx, in.Actor, key, hash, items)
	}
	return created, existing, err
}

func (s *Service) create(ctx context.Context, actor Actor, key, hash string, items []CreateItem) (Order, bool, error) {
	var (
		result   Order
		existing bool
	)

	err := postgres.InTx(ctx, s.repo.Pool(), func(ctx context.Context, tx pgx.Tx) error {
		existingID, storedHash, found, err := s.repo.FindIdempotentOrder(ctx, tx, actor.UserID, key)
		if err != nil {
			return err
		}
		if found {
			if storedHash != hash {
				return ErrIdempotencyKeyReused
			}
			order, err := s.repo.GetByID(ctx, existingID)
			if err != nil {
				return err
			}
			result, existing = order, true
			return nil
		}

		order := Order{
			ID:       uuid.New(),
			UserID:   actor.UserID,
			Status:   StatusNew,
			Currency: events.Currency,
			Items:    make([]Item, 0, len(items)),
		}
		for _, it := range items {
			order.Items = append(order.Items, Item{
				ID:        uuid.New(),
				ProductID: uuid.MustParse(it.ProductID),
				Quantity:  it.Quantity,
			})
		}

		if err := s.repo.Insert(ctx, tx, order); err != nil {
			return err
		}
		if err := s.repo.SaveIdempotencyKey(ctx, tx, actor.UserID, key, hash, order.ID); err != nil {
			return err
		}

		evt := events.OrderCreated{
			Envelope: events.NewEnvelope(events.TypeOrderCreated, order.ID.String()),
			UserID:   order.UserID.String(),
			Items:    make([]events.Item, 0, len(items)),
		}
		for _, it := range items {
			evt.Items = append(evt.Items, events.Item{ProductID: it.ProductID, Quantity: it.Quantity})
		}

		msg, err := outbox.FromEvent(events.TopicOrderCreated, evt.Envelope, evt)
		if err != nil {
			return err
		}
		if err := outbox.Enqueue(ctx, tx, msg); err != nil {
			return err
		}

		// Заказ сохранён и событие лежит в outbox в одной транзакции:
		// потерять его после коммита уже нельзя.
		logger.WithOrderID(logger.From(ctx), order.ID.String()).Info("заказ создан",
			slog.String(logger.KeyUserID, order.UserID.String()),
			slog.Int("items", len(order.Items)),
		)

		result = order
		return nil
	})
	if err != nil {
		return Order{}, false, err
	}
	return result, existing, nil
}

// Get возвращает заказ с проверкой прав доступа.
func (s *Service) Get(ctx context.Context, actor Actor, id uuid.UUID) (Order, error) {
	o, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return Order{}, err
	}
	if !actor.CanAccess(o) {
		// Чужой заказ выглядит как несуществующий: так не утекает сам факт
		// наличия заказа у другого пользователя.
		return Order{}, ErrNotFound
	}
	return o, nil
}

// List возвращает заказы: обычному пользователю — только свои, ADMIN — все.
func (s *Service) List(ctx context.Context, actor Actor, status Status, limit, offset int) ([]Order, int, error) {
	if status != "" && !status.Valid() {
		return nil, 0, invalid("status", "неизвестный статус %q", status)
	}

	f := ListFilter{Status: status, Limit: limit, Offset: offset}
	if actor.Role != auth.RoleAdmin {
		userID := actor.UserID
		f.UserID = &userID
	}
	return s.repo.List(ctx, f)
}

// Cancel отменяет заказ и публикует компенсирующее событие order.cancelled:
// product-service вернёт остаток, если бронь была.
func (s *Service) Cancel(ctx context.Context, actor Actor, id uuid.UUID, reason string) (Order, error) {
	if strings.TrimSpace(reason) == "" {
		reason = ReasonCancelledByUser
	}

	var result Order

	err := postgres.InTx(ctx, s.repo.Pool(), func(ctx context.Context, tx pgx.Tx) error {
		o, err := s.repo.LockByID(ctx, tx, id)
		if err != nil {
			return err
		}
		if !actor.CanAccess(o) {
			return ErrNotFound
		}
		if !o.Status.CanTransitionTo(StatusCancelled) {
			return ErrInvalidTransition{From: o.Status, To: StatusCancelled}
		}

		previous := o.Status
		if err := s.repo.UpdateStatus(ctx, tx, o.ID, StatusCancelled, o.TotalAmountCents, reason); err != nil {
			return err
		}

		cancelled := events.OrderCancelled{
			Envelope:         events.NewEnvelope(events.TypeOrderCancelled, o.ID.String()),
			UserID:           o.UserID.String(),
			PreviousStatus:   previous.String(),
			Reason:           reason,
			StockWasReserved: previous == StatusReserved,
		}
		cancelMsg, err := outbox.FromEvent(events.TopicOrderCancelled, cancelled.Envelope, cancelled)
		if err != nil {
			return err
		}
		if err := outbox.Enqueue(ctx, tx, cancelMsg); err != nil {
			return err
		}

		if err := s.enqueueStatusChanged(ctx, tx, o, previous, StatusCancelled, reason); err != nil {
			return err
		}

		o.Status = StatusCancelled
		o.CancelReason = reason
		result = o

		logger.WithOrderID(logger.From(ctx), o.ID.String()).Info("заказ отменён",
			slog.String("previous_status", previous.String()),
			slog.String("reason", reason),
		)
		return nil
	})
	if err != nil {
		return Order{}, err
	}

	return s.repo.GetByID(ctx, result.ID)
}

// Complete завершает заказ. Используется, когда автозавершение отключено
// (ORDER_AUTO_COMPLETE=false).
func (s *Service) Complete(ctx context.Context, actor Actor, id uuid.UUID) (Order, error) {
	err := postgres.InTx(ctx, s.repo.Pool(), func(ctx context.Context, tx pgx.Tx) error {
		o, err := s.repo.LockByID(ctx, tx, id)
		if err != nil {
			return err
		}
		if !actor.CanAccess(o) {
			return ErrNotFound
		}
		if !o.Status.CanTransitionTo(StatusCompleted) {
			return ErrInvalidTransition{From: o.Status, To: StatusCompleted}
		}
		return s.transition(ctx, tx, o, StatusCompleted, "")
	})
	if err != nil {
		return Order{}, err
	}
	return s.repo.GetByID(ctx, id)
}

// HandleStockReserved переводит заказ в RESERVED и, если включено
// автозавершение, сразу в COMPLETED.
func (s *Service) HandleStockReserved(ctx context.Context, evt events.StockReserved) error {
	orderID, err := uuid.Parse(evt.OrderID)
	if err != nil {
		return fmt.Errorf("order_id %q не является UUID", evt.OrderID)
	}

	log := logger.WithOrderID(logger.From(ctx), evt.OrderID)

	return postgres.InTx(ctx, s.repo.Pool(), func(ctx context.Context, tx pgx.Tx) error {
		first, err := s.markProcessed(ctx, tx, evt.EventID, events.TopicStockReserved, log)
		if err != nil || !first {
			return err
		}

		o, err := s.repo.LockByID(ctx, tx, orderID)
		if err != nil {
			return err
		}

		if o.Status == StatusCancelled {
			// Заказ успели отменить: событие order.cancelled уже опубликовано
			// и уйдёт в тот же ключ партиции, поэтому остаток вернётся.
			log.Warn("резерв получен по отменённому заказу, компенсация уже запланирована")
			return nil
		}
		if !o.Status.CanTransitionTo(StatusReserved) {
			log.Warn("резерв получен в неподходящем статусе, событие пропущено",
				slog.String("status", o.Status.String()))
			return nil
		}

		if err := s.repo.ApplyReservedPrices(ctx, tx, o.ID, evt.Items); err != nil {
			return err
		}

		o.TotalAmountCents = evt.TotalAmountCents
		if err := s.transition(ctx, tx, o, StatusReserved, ""); err != nil {
			return err
		}

		log.Info("заказ переведён в RESERVED",
			slog.Int64("total_amount_cents", evt.TotalAmountCents))

		if !s.autoComplete {
			return nil
		}

		o.Status = StatusReserved
		if err := s.transition(ctx, tx, o, StatusCompleted, ""); err != nil {
			return err
		}

		log.Info("заказ автоматически переведён в COMPLETED")
		return nil
	})
}

// HandleStockRejected отменяет заказ, если товара не хватило.
func (s *Service) HandleStockRejected(ctx context.Context, evt events.StockRejected) error {
	orderID, err := uuid.Parse(evt.OrderID)
	if err != nil {
		return fmt.Errorf("order_id %q не является UUID", evt.OrderID)
	}

	log := logger.WithOrderID(logger.From(ctx), evt.OrderID)

	return postgres.InTx(ctx, s.repo.Pool(), func(ctx context.Context, tx pgx.Tx) error {
		first, err := s.markProcessed(ctx, tx, evt.EventID, events.TopicStockRejected, log)
		if err != nil || !first {
			return err
		}

		o, err := s.repo.LockByID(ctx, tx, orderID)
		if err != nil {
			return err
		}

		if o.Status == StatusCancelled {
			log.Info("заказ уже отменён, отказ в резерве повторно не применяется")
			return nil
		}
		if !o.Status.CanTransitionTo(StatusCancelled) {
			log.Warn("отказ в резерве получен в неподходящем статусе, событие пропущено",
				slog.String("status", o.Status.String()))
			return nil
		}

		reason := ReasonStockRejected
		if evt.Message != "" {
			reason = evt.Message
		}

		if err := s.transition(ctx, tx, o, StatusCancelled, reason); err != nil {
			return err
		}

		log.Info("заказ отменён из-за отказа в резерве",
			slog.String("reason", evt.Reason),
			slog.Int("rejected_items", len(evt.Items)),
		)
		return nil
	})
}

// transition выполняет переход статуса и кладёт order.status-changed в outbox.
func (s *Service) transition(ctx context.Context, tx pgx.Tx, o Order, next Status, reason string) error {
	if !o.Status.CanTransitionTo(next) {
		return ErrInvalidTransition{From: o.Status, To: next}
	}
	if err := s.repo.UpdateStatus(ctx, tx, o.ID, next, o.TotalAmountCents, reason); err != nil {
		return err
	}
	return s.enqueueStatusChanged(ctx, tx, o, o.Status, next, reason)
}

func (s *Service) enqueueStatusChanged(ctx context.Context, tx pgx.Tx, o Order, from, to Status, reason string) error {
	evt := events.OrderStatusChanged{
		Envelope:         events.NewEnvelope(events.TypeOrderStatusChanged, o.ID.String()),
		UserID:           o.UserID.String(),
		OldStatus:        from.String(),
		NewStatus:        to.String(),
		TotalAmountCents: o.TotalAmountCents,
		Reason:           reason,
	}

	msg, err := outbox.FromEvent(events.TopicOrderStatusChanged, evt.Envelope, evt)
	if err != nil {
		return err
	}
	return outbox.Enqueue(ctx, tx, msg)
}

// markProcessed отмечает событие обработанным; false означает дубликат.
func (s *Service) markProcessed(ctx context.Context, tx pgx.Tx, eventID, topic string, log *slog.Logger) (bool, error) {
	first, err := idempotency.MarkProcessed(ctx, tx, s.consumerGroup, eventID, topic)
	if err != nil {
		return false, err
	}
	if !first {
		log.Info("событие уже обработано, пропускаем",
			slog.String(logger.KeyEventID, eventID),
			slog.String(logger.KeyTopic, topic),
		)
	}
	return first, nil
}
