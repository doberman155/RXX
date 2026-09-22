package order

import (
	"context"
	"fmt"

	"github.com/hshsb/shop/pkg/events"
	"github.com/hshsb/shop/pkg/kafkax"
)

// EventHandler маршрутизирует ответы product-service в методы саги.
func EventHandler(svc *Service) kafkax.Handler {
	return func(ctx context.Context, msg kafkax.Inbound) error {
		switch msg.Topic {
		case events.TopicStockReserved:
			evt, err := events.Decode[events.StockReserved](msg.Value)
			if err != nil {
				return kafkax.NonRetryable(err)
			}
			return svc.HandleStockReserved(ctx, evt)

		case events.TopicStockRejected:
			evt, err := events.Decode[events.StockRejected](msg.Value)
			if err != nil {
				return kafkax.NonRetryable(err)
			}
			return svc.HandleStockRejected(ctx, evt)

		default:
			return kafkax.NonRetryable(fmt.Errorf("неожиданный топик %q", msg.Topic))
		}
	}
}
