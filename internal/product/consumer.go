package product

import (
	"context"
	"fmt"

	"github.com/hshsb/shop/pkg/events"
	"github.com/hshsb/shop/pkg/kafkax"
)

// EventHandler маршрутизирует сообщения Kafka в методы сервиса.
// Сообщение с неизвестным топиком или битым JSON повторять бессмысленно —
// оно сразу уезжает в DLQ.
func EventHandler(svc *Service) kafkax.Handler {
	return func(ctx context.Context, msg kafkax.Inbound) error {
		switch msg.Topic {
		case events.TopicOrderCreated:
			evt, err := events.Decode[events.OrderCreated](msg.Value)
			if err != nil {
				return kafkax.NonRetryable(err)
			}
			return svc.HandleOrderCreated(ctx, evt)

		case events.TopicOrderCancelled:
			evt, err := events.Decode[events.OrderCancelled](msg.Value)
			if err != nil {
				return kafkax.NonRetryable(err)
			}
			return svc.HandleOrderCancelled(ctx, evt)

		default:
			return kafkax.NonRetryable(fmt.Errorf("неожиданный топик %q", msg.Topic))
		}
	}
}
