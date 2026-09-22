package kafkax

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	"github.com/hshsb/shop/pkg/config"
	"github.com/hshsb/shop/pkg/logger"
)

func testLogger() *slog.Logger { return logger.NewWithWriter(io.Discard, "test", "error") }

func testKafkaConfig() config.Kafka {
	return config.Kafka{
		Brokers:    []string{"localhost:9092"},
		MaxRetries: 3,
		DLQSuffix:  ".dlq",
		MinBytes:   1,
		MaxBytes:   1024,
	}
}

func TestNonRetryableПомечаетОшибку(t *testing.T) {
	t.Parallel()

	base := errors.New("битый JSON")
	wrapped := NonRetryable(base)

	require.ErrorIs(t, wrapped, ErrNonRetryable)
	require.Contains(t, wrapped.Error(), "битый JSON")

	require.NoError(t, NonRetryable(nil))
}

func TestКонвертацияЗаголовков(t *testing.T) {
	t.Parallel()

	require.Nil(t, toKafkaHeaders(nil))
	require.Nil(t, fromKafkaHeaders(nil))

	headers := map[string]string{"event-id": "e-1", "event-type": "order.created"}
	kafkaHeaders := toKafkaHeaders(headers)
	require.Len(t, kafkaHeaders, 2)

	require.Equal(t, headers, fromKafkaHeaders(kafkaHeaders))
}

func TestFromKafkaHeadersСохраняетЗначения(t *testing.T) {
	t.Parallel()

	got := fromKafkaHeaders([]kafka.Header{{Key: "x-error", Value: []byte("ошибка")}})
	require.Equal(t, map[string]string{"x-error": "ошибка"}, got)
}

func TestDesiredTopicsСоздаётТопикиИDLQ(t *testing.T) {
	t.Parallel()

	specs := DesiredTopics(config.Kafka{Partitions: 3, ReplicationFactor: 1, DLQSuffix: ".dlq"})

	byName := make(map[string]TopicSpec, len(specs))
	for _, s := range specs {
		byName[s.Name] = s
	}

	require.Len(t, specs, 10, "пять бизнес-топиков и пять DLQ")
	require.Equal(t, 3, byName["order.created"].Partitions)
	require.Equal(t, 1, byName["order.created.dlq"].Partitions, "в DLQ порядок не важен")
	require.Contains(t, byName, "stock.reserved.dlq")
	require.Contains(t, byName, "order.status-changed.dlq")
}

func TestNewConsumerПроверяетПараметры(t *testing.T) {
	t.Parallel()

	cfg := testKafkaConfig()
	producer := NewProducer(cfg, testLogger())
	t.Cleanup(func() { _ = producer.Close() })

	noop := func(ctx context.Context, msg Inbound) error { return nil }

	_, err := NewConsumer(cfg, ConsumerOptions{GroupID: "g"}, noop, producer, testLogger())
	require.Error(t, err, "без топиков консьюмер создаваться не должен")

	_, err = NewConsumer(cfg, ConsumerOptions{Topics: []string{"t"}}, noop, producer, testLogger())
	require.Error(t, err, "без group id консьюмер создаваться не должен")

	_, err = NewConsumer(cfg, ConsumerOptions{Topics: []string{"t"}, GroupID: "g"}, nil, producer, testLogger())
	require.Error(t, err, "без обработчика консьюмер создаваться не должен")

	_, err = NewConsumer(cfg, ConsumerOptions{Topics: []string{"t"}, GroupID: "g"}, noop, nil, testLogger())
	require.Error(t, err, "без продюсера DLQ консьюмер создаваться не должен")
}

func TestNewConsumerПодставляетТаймаутОбработки(t *testing.T) {
	t.Parallel()

	cfg := testKafkaConfig()
	producer := NewProducer(cfg, testLogger())
	t.Cleanup(func() { _ = producer.Close() })

	consumer, err := NewConsumer(cfg, ConsumerOptions{Topics: []string{"t"}, GroupID: "g"},
		func(ctx context.Context, msg Inbound) error { return nil }, producer, testLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = consumer.reader.Close() })

	require.Positive(t, consumer.processTimeout)
	require.Equal(t, cfg.MaxRetries, consumer.maxRetries)
	require.Equal(t, ".dlq", consumer.dlqSuffix)
}

func TestPublishОтклоняетСообщениеБезТопика(t *testing.T) {
	t.Parallel()

	producer := NewProducer(testKafkaConfig(), testLogger())
	t.Cleanup(func() { _ = producer.Close() })

	err := producer.Publish(context.Background(), Message{Key: "k", Value: []byte("{}")})
	require.Error(t, err)
	require.Contains(t, err.Error(), "пустой топик")
}

func TestPublishБезСообщенийНичегоНеДелает(t *testing.T) {
	t.Parallel()

	producer := NewProducer(testKafkaConfig(), testLogger())
	t.Cleanup(func() { _ = producer.Close() })

	require.NoError(t, producer.Publish(context.Background()))
}
