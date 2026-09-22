package kafkax

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/hshsb/shop/pkg/config"
	"github.com/hshsb/shop/pkg/events"
)

// TopicSpec описывает создаваемый топик.
type TopicSpec struct {
	Name              string
	Partitions        int
	ReplicationFactor int
}

// DesiredTopics возвращает список всех топиков проекта вместе с их DLQ.
func DesiredTopics(cfg config.Kafka) []TopicSpec {
	topics := events.Topics()
	specs := make([]TopicSpec, 0, len(topics)*2)

	for _, t := range topics {
		specs = append(specs,
			TopicSpec{Name: t, Partitions: cfg.Partitions, ReplicationFactor: cfg.ReplicationFactor},
			// DLQ достаточно одной партиции: порядок там не важен.
			TopicSpec{Name: t + cfg.DLQSuffix, Partitions: 1, ReplicationFactor: cfg.ReplicationFactor},
		)
	}
	return specs
}

// EnsureTopics создаёт недостающие топики. Уже существующие пропускаются,
// поэтому вызов идемпотентен и безопасен при каждом старте.
func EnsureTopics(ctx context.Context, cfg config.Kafka, specs []TopicSpec, log *slog.Logger) error {
	client := &kafka.Client{
		Addr:    kafka.TCP(cfg.Brokers...),
		Timeout: 15 * time.Second,
	}

	configs := make([]kafka.TopicConfig, 0, len(specs))
	for _, s := range specs {
		configs = append(configs, kafka.TopicConfig{
			Topic:             s.Name,
			NumPartitions:     s.Partitions,
			ReplicationFactor: s.ReplicationFactor,
		})
	}

	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: configs})
	if err != nil {
		return fmt.Errorf("создать топики: %w", err)
	}

	var problems []error
	for topic, topicErr := range resp.Errors {
		switch {
		case topicErr == nil:
			log.Info("топик создан", slog.String("topic", topic))
		case errors.Is(topicErr, kafka.TopicAlreadyExists):
			log.Info("топик уже существует", slog.String("topic", topic))
		default:
			problems = append(problems, fmt.Errorf("%s: %w", topic, topicErr))
		}
	}

	return errors.Join(problems...)
}

// WaitForBroker дожидается доступности брокера — в docker compose сервисы
// стартуют раньше, чем Kafka готова принимать соединения.
func WaitForBroker(ctx context.Context, cfg config.Kafka, timeout time.Duration, log *slog.Logger) error {
	deadline := time.Now().Add(timeout)
	backoff := 500 * time.Millisecond

	for attempt := 1; ; attempt++ {
		err := pingBroker(ctx, cfg)
		if err == nil {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("kafka недоступна после %d попыток: %w", attempt, err)
		}

		log.Warn("kafka пока недоступна, повтор",
			slog.Int("attempt", attempt),
			slog.String("error", err.Error()),
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

func pingBroker(ctx context.Context, cfg config.Kafka) error {
	client := &kafka.Client{Addr: kafka.TCP(cfg.Brokers...), Timeout: 5 * time.Second}

	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if _, err := client.Metadata(reqCtx, &kafka.MetadataRequest{}); err != nil {
		return fmt.Errorf("запросить метаданные кластера: %w", err)
	}
	return nil
}

// Ping пригоден для readiness-пробы сервиса.
func Ping(cfg config.Kafka) func(ctx context.Context) error {
	return func(ctx context.Context) error { return pingBroker(ctx, cfg) }
}
