package notification

import (
	"context"
	"fmt"

	"github.com/hshsb/shop/pkg/events"
	"github.com/hshsb/shop/pkg/kafkax"
)

// EventHandler маршрутизирует события заказа в методы сервиса.
func EventHandler(svc *Service) kafkax.Handler {
	return func(ctx context.Context, msg kafkax.Inbound) error {
		switch msg.Topic {
		case events.TopicOrderCreated:
			evt, err := events.Decode[events.OrderCreated](msg.Value)
			if err != nil {
				return kafkax.NonRetryable(err)
			}
			return svc.HandleOrderCreated(ctx, evt)

		case events.TopicOrderStatusChanged:
			evt, err := events.Decode[events.OrderStatusChanged](msg.Value)
			if err != nil {
				return kafkax.NonRetryable(err)
			}
			return svc.HandleOrderStatusChanged(ctx, evt)

		default:
			return kafkax.NonRetryable(fmt.Errorf("неожиданный топик %q", msg.Topic))
		}
	}
}
