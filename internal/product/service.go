package product

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/hshsb/shop/pkg/events"
	"github.com/hshsb/shop/pkg/idempotency"
	"github.com/hshsb/shop/pkg/logger"
	"github.com/hshsb/shop/pkg/outbox"
	"github.com/hshsb/shop/pkg/postgres"
)

// Service — бизнес-логика product-service.
type Service struct {
	repo *Repository
	// consumerGroup нужен для записи в processed_events.
	consumerGroup string
	log           *slog.Logger
}

// NewService собирает сервис.
func NewService(repo *Repository, consumerGroup string, log *slog.Logger) *Service {
	return &Service{repo: repo, consumerGroup: consumerGroup, log: log}
}

// Create добавляет товар в каталог.
func (s *Service) Create(ctx context.Context, in CreateInput) (Product, error) {
	if err := in.Validate(); err != nil {
		return Product{}, err
	}

	return s.repo.Create(ctx, Product{
		ID:          uuid.New(),
		SKU:         strings.TrimSpace(in.SKU),
		Name:        strings.TrimSpace(in.Name),
		Description: in.Description,
		Category:    strings.TrimSpace(in.Category),
		PriceCents:  in.PriceCents,
		Stock:       in.Stock,
	})
}

// Get возвращает товар по идентификатору.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (Product, error) {
	return s.repo.GetByID(ctx, id)
}

// Update обновляет карточку товара.
func (s *Service) Update(ctx context.Context, id uuid.UUID, in UpdateInput) (Product, error) {
	if err := in.Validate(); err != nil {
		return Product{}, err
	}
	in.Name = strings.TrimSpace(in.Name)
	in.Category = strings.TrimSpace(in.Category)
	return s.repo.Update(ctx, id, in)
}

// Delete помечает товар удалённым.
func (s *Service) Delete(ctx context.Context, id uuid.UUID) error {
	return s.repo.SoftDelete(ctx, id)
}

// List возвращает страницу каталога.
func (s *Service) List(ctx context.Context, f ListFilter) ([]Product, int, error) {
	return s.repo.List(ctx, f)
}

// HandleOrderCreated резервирует товары под заказ и публикует
// stock.reserved либо stock.rejected. Вся работа — в одной транзакции
// вместе с отметкой об обработке события и записью в outbox.
func (s *Service) HandleOrderCreated(ctx context.Context, evt events.OrderCreated) error {
	orderID, err := uuid.Parse(evt.OrderID)
	if err != nil {
		return fmt.Errorf("order_id %q не является UUID", evt.OrderID)
	}

	items, err := aggregateItems(evt.Items)
	if err != nil {
		return err
	}

	log := logger.WithOrderID(logger.From(ctx), evt.OrderID)

	return postgres.InTx(ctx, s.repo.Pool(), func(ctx context.Context, tx pgx.Tx) error {
		first, err := idempotency.MarkProcessed(ctx, tx, s.consumerGroup, evt.EventID, events.TopicOrderCreated)
		if err != nil {
			return err
		}
		if !first {
			log.Info("событие уже обработано, пропускаем",
				slog.String(logger.KeyEventID, evt.EventID),
				slog.String(logger.KeyEventType, evt.EventType),
			)
			return nil
		}

		// Защита от второго резерва по тому же заказу, если событие пришло
		// с другим event_id (например, после ручной перепубликации).
		alreadyReserved, err := s.repo.HasReservation(ctx, tx, orderID)
		if err != nil {
			return err
		}
		if alreadyReserved {
			log.Warn("по заказу уже есть бронь, повторный резерв не выполняется")
			return nil
		}

		outcome, err := s.repo.Reserve(ctx, tx, orderID, items)
		if err != nil {
			return err
		}

		if outcome.Reserved {
			reserved := events.StockReserved{
				Envelope:         events.NewEnvelope(events.TypeStockReserved, evt.OrderID),
				Items:            outcome.Items,
				TotalAmountCents: outcome.TotalCents,
				Currency:         events.Currency,
			}
			msg, err := outbox.FromEvent(events.TopicStockReserved, reserved.Envelope, reserved)
			if err != nil {
				return err
			}
			if err := outbox.Enqueue(ctx, tx, msg); err != nil {
				return err
			}

			log.Info("товары зарезервированы",
				slog.Int("items", len(outcome.Items)),
				slog.Int64("total_amount_cents", outcome.TotalCents),
			)
			return nil
		}

		rejected := events.StockRejected{
			Envelope: events.NewEnvelope(events.TypeStockRejected, evt.OrderID),
			Reason:   outcome.Reason,
			Message:  outcome.Message,
			Items:    outcome.Rejected,
		}
		msg, err := outbox.FromEvent(events.TopicStockRejected, rejected.Envelope, rejected)
		if err != nil {
			return err
		}
		if err := outbox.Enqueue(ctx, tx, msg); err != nil {
			return err
		}

		log.Info("в резерве отказано",
			slog.String("reason", outcome.Reason),
			slog.Int("rejected_items", len(outcome.Rejected)),
		)
		return nil
	})
}

// HandleOrderCancelled — компенсация саги: заказ отменён, остатки возвращаются.
func (s *Service) HandleOrderCancelled(ctx context.Context, evt events.OrderCancelled) error {
	orderID, err := uuid.Parse(evt.OrderID)
	if err != nil {
		return fmt.Errorf("order_id %q не является UUID", evt.OrderID)
	}

	log := logger.WithOrderID(logger.From(ctx), evt.OrderID)

	return postgres.InTx(ctx, s.repo.Pool(), func(ctx context.Context, tx pgx.Tx) error {
		first, err := idempotency.MarkProcessed(ctx, tx, s.consumerGroup, evt.EventID, events.TopicOrderCancelled)
		if err != nil {
			return err
		}
		if !first {
			log.Info("событие уже обработано, пропускаем",
				slog.String(logger.KeyEventID, evt.EventID),
				slog.String(logger.KeyEventType, evt.EventType),
			)
			return nil
		}

		released, err := s.repo.Release(ctx, tx, orderID)
		if err != nil {
			return err
		}

		if len(released) == 0 {
			log.Info("активных броней по заказу нет, возвращать нечего")
			return nil
		}

		for _, item := range released {
			log.Info("остаток возвращён на склад",
				slog.String("product_id", item.ProductID.String()),
				slog.Int("quantity", item.Quantity),
			)
		}
		return nil
	})
}
