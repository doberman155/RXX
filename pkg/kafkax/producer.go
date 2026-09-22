package kafkax

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"

	"github.com/hshsb/shop/pkg/config"
)

// Producer публикует сообщения в Kafka. Используется релеером outbox и
// отправкой в DLQ; напрямую из HTTP-обработчиков публиковать нельзя.
type Producer struct {
	writer *kafka.Writer
	log    *slog.Logger
}

// NewProducer создаёт продюсер. Balancer — Hash: сообщения с одинаковым
// ключом (orderId) всегда попадают в одну партицию.
func NewProducer(cfg config.Kafka, log *slog.Logger) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(cfg.Brokers...),
			Balancer:               &kafka.Hash{},
			RequiredAcks:           kafka.RequireAll,
			WriteTimeout:           cfg.WriteTimeout,
			AllowAutoTopicCreation: true,
			Async:                  false,
			Compression:            kafka.Snappy,
			ErrorLogger:            kafka.LoggerFunc(func(msg string, args ...any) { log.Error("kafka-writer: " + fmt.Sprintf(msg, args...)) }),
		},
		log: log,
	}
}

// Publish синхронно отправляет сообщения и возвращает ошибку, если хотя бы
// одно не удалось записать. Вызывающий код решает, повторять ли отправку.
func (p *Producer) Publish(ctx context.Context, msgs ...Message) error {
	if len(msgs) == 0 {
		return nil
	}

	batch := make([]kafka.Message, 0, len(msgs))
	for _, m := range msgs {
		if m.Topic == "" {
			return fmt.Errorf("kafka: пустой топик у сообщения с ключом %q", m.Key)
		}
		batch = append(batch, kafka.Message{
			Topic:   m.Topic,
			Key:     []byte(m.Key),
			Value:   m.Value,
			Headers: toKafkaHeaders(m.Headers),
		})
	}

	if err := p.writer.WriteMessages(ctx, batch...); err != nil {
		return fmt.Errorf("опубликовать %d сообщений: %w", len(batch), err)
	}
	return nil
}

// Close закрывает продюсер, дожидаясь отправки буферов.
func (p *Producer) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("закрыть kafka-writer: %w", err)
	}
	return nil
}
