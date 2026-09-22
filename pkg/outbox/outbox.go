// Package outbox реализует transactional outbox: событие записывается в
// таблицу outbox в той же транзакции, что и бизнес-данные, а фоновый
// релеер отправляет его в Kafka и помечает как отправленное.
// Прямая публикация в Kafka из HTTP-обработчика не допускается.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/hshsb/shop/pkg/events"
)

// Message — запись, которую нужно опубликовать в Kafka.
type Message struct {
	EventID string
	Topic   string
	// Key — ключ партиционирования, всегда orderId.
	Key     string
	Payload []byte
	Headers map[string]string
}

// FromEvent собирает запись outbox из события: ключ — order_id, заголовки
// заполняются идентификатором и типом события.
func FromEvent(topic string, env events.Envelope, evt any) (Message, error) {
	payload, err := events.Encode(evt)
	if err != nil {
		return Message{}, err
	}
	return Message{
		EventID: env.EventID,
		Topic:   topic,
		Key:     env.OrderID,
		Payload: payload,
		Headers: map[string]string{
			"event-id":   env.EventID,
			"event-type": env.EventType,
		},
	}, nil
}

// Enqueue сохраняет сообщения в outbox. Вызывается только внутри
// транзакции, изменяющей бизнес-данные.
func Enqueue(ctx context.Context, tx pgx.Tx, msgs ...Message) error {
	const query = `
		INSERT INTO outbox (event_id, topic, partition_key, payload, headers)
		VALUES ($1, $2, $3, $4, $5)`

	for _, m := range msgs {
		if m.Topic == "" || m.Key == "" || m.EventID == "" {
			return fmt.Errorf("outbox: некорректное сообщение (topic=%q key=%q event_id=%q)", m.Topic, m.Key, m.EventID)
		}

		headers, err := json.Marshal(m.Headers)
		if err != nil {
			return fmt.Errorf("outbox: сериализовать заголовки: %w", err)
		}

		if _, err := tx.Exec(ctx, query, m.EventID, m.Topic, m.Key, m.Payload, headers); err != nil {
			return fmt.Errorf("outbox: сохранить событие %s: %w", m.EventID, err)
		}
	}
	return nil
}
