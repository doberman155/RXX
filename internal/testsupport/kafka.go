//go:build integration

package testsupport

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/hshsb/shop/pkg/config"
	"github.com/hshsb/shop/pkg/kafkax"
)

// DefaultKafkaBrokers указывает на внешний listener Redpanda из docker-compose.
const DefaultKafkaBrokers = "localhost:19092"

// KafkaBrokers возвращает список брокеров; переопределяется TEST_KAFKA_BROKERS.
func KafkaBrokers() []string {
	raw := strings.TrimSpace(os.Getenv("TEST_KAFKA_BROKERS"))
	if raw == "" {
		raw = DefaultKafkaBrokers
	}

	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// KafkaConfig возвращает конфигурацию Kafka для теста.
func KafkaConfig() config.Kafka {
	return config.Kafka{
		Brokers:           KafkaBrokers(),
		MaxRetries:        3,
		RetryBackoff:      50 * time.Millisecond,
		MaxRetryBackoff:   200 * time.Millisecond,
		DLQSuffix:         ".dlq",
		Partitions:        1,
		ReplicationFactor: 1,
		WriteTimeout:      10 * time.Second,
		MinBytes:          1,
		MaxBytes:          1 << 20,
	}
}

// RequireKafka пропускает тест, если брокер недоступен.
func RequireKafka(t testing.TB) config.Kafka {
	t.Helper()

	cfg := KafkaConfig()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := kafkax.WaitForBroker(ctx, cfg, 8*time.Second, Logger()); err != nil {
		t.Skipf("Kafka недоступна (%v). Поднимите стенд: docker compose up -d redpanda", err)
	}
	return cfg
}

// NewTopic создаёт уникальный топик вместе с его DLQ и возвращает его имя.
func NewTopic(t testing.TB, cfg config.Kafka, prefix string) string {
	t.Helper()

	name := prefix + "." + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	specs := []kafkax.TopicSpec{
		{Name: name, Partitions: 1, ReplicationFactor: 1},
		{Name: name + cfg.DLQSuffix, Partitions: 1, ReplicationFactor: 1},
	}
	if err := kafkax.EnsureTopics(ctx, cfg, specs, Logger()); err != nil {
		t.Fatalf("создать тестовый топик: %v", err)
	}
	return name
}
