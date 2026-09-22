//go:build integration

// Package integration_test проверяет взаимодействие с реальными Kafka и
// PostgreSQL: публикацию через outbox, ретраи, dead-letter topic и
// порядок сообщений одного заказа.
package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	"github.com/hshsb/shop/internal/testsupport"
	"github.com/hshsb/shop/migrations"
	"github.com/hshsb/shop/pkg/config"
	"github.com/hshsb/shop/pkg/events"
	"github.com/hshsb/shop/pkg/kafkax"
	"github.com/hshsb/shop/pkg/outbox"
	"github.com/hshsb/shop/pkg/postgres"
)

// readMessages читает ожидаемое количество сообщений из топика.
func readMessages(t *testing.T, cfg config.Kafka, topic string, count int, timeout time.Duration) []kafka.Message {
	t.Helper()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   cfg.Brokers,
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  1 << 20,
		MaxWait:   200 * time.Millisecond,
	})
	defer func() { _ = reader.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var out []kafka.Message
	for len(out) < count {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("прочитано %d из %d сообщений топика %s: %v", len(out), count, topic, err)
		}
		out = append(out, msg)
	}
	return out
}

func headerValue(msg kafka.Message, key string) string {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func TestOutboxРелеерПубликуетСобытиеИПомечаетЕгоОтправленным(t *testing.T) {
	cfg := testsupport.RequireKafka(t)
	pool := testsupport.NewDatabase(t, migrations.DirProduct)
	topic := testsupport.NewTopic(t, cfg, "test.outbox")

	producer := kafkax.NewProducer(cfg, testsupport.Logger())
	t.Cleanup(func() { _ = producer.Close() })

	orderID := uuid.NewString()
	evt := events.StockReserved{
		Envelope:         events.NewEnvelope(events.TypeStockReserved, orderID),
		Items:            []events.ReservedItem{{ProductID: uuid.NewString(), Quantity: 1, UnitPriceCents: 100}},
		TotalAmountCents: 100,
		Currency:         events.Currency,
	}

	msg, err := outbox.FromEvent(topic, evt.Envelope, evt)
	require.NoError(t, err)

	// Событие пишется в outbox в той же транзакции, что и бизнес-данные.
	require.NoError(t, postgres.InTx(context.Background(), pool, func(ctx context.Context, tx pgx.Tx) error {
		return outbox.Enqueue(ctx, tx, msg)
	}))

	relayer := outbox.NewRelayer(pool, producer, config.Outbox{
		PollInterval: 100 * time.Millisecond,
		BatchSize:    10,
		MaxAttempts:  5,
		CleanupAfter: time.Hour,
	}, testsupport.Logger())

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, relayer.Run(ctx))
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	published := readMessages(t, cfg, topic, 1, 30*time.Second)
	require.Equal(t, orderID, string(published[0].Key), "ключ сообщения — идентификатор заказа")
	require.Equal(t, evt.EventID, headerValue(published[0], "event-id"))
	require.Equal(t, events.TypeStockReserved, headerValue(published[0], "event-type"))

	decoded, err := events.Decode[events.StockReserved](published[0].Value)
	require.NoError(t, err)
	require.Equal(t, evt.EventID, decoded.EventID)

	require.Eventually(t, func() bool {
		var unsent int
		if err := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM outbox WHERE sent_at IS NULL`).Scan(&unsent); err != nil {
			return false
		}
		return unsent == 0
	}, 15*time.Second, 200*time.Millisecond, "запись outbox должна быть помечена отправленной")

	// Релеер не должен публиковать одно и то же дважды.
	time.Sleep(700 * time.Millisecond)

	var total int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox`).Scan(&total))
	require.Equal(t, 1, total)
}

func TestКонсьюмерПовторяетОбработкуИОтправляетВDLQ(t *testing.T) {
	cfg := testsupport.RequireKafka(t)
	topic := testsupport.NewTopic(t, cfg, "test.retry")

	producer := kafkax.NewProducer(cfg, testsupport.Logger())
	t.Cleanup(func() { _ = producer.Close() })

	orderID := uuid.NewString()
	require.NoError(t, producer.Publish(context.Background(), kafkax.Message{
		Topic: topic,
		Key:   orderID,
		Value: []byte(`{"нерабочее":"событие"}`),
	}))

	var attempts atomic.Int32
	handler := func(ctx context.Context, msg kafkax.Inbound) error {
		attempts.Add(1)
		return errors.New("обработчик всегда падает")
	}

	consumer, err := kafkax.NewConsumer(cfg, kafkax.ConsumerOptions{
		Topics:  []string{topic},
		GroupID: "test-retry-" + uuid.NewString()[:8],
	}, handler, producer, testsupport.Logger())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	dead := readMessages(t, cfg, topic+cfg.DLQSuffix, 1, 45*time.Second)

	require.Equal(t, orderID, string(dead[0].Key), "ключ сохраняется, чтобы заказ можно было найти")
	require.Equal(t, `{"нерабочее":"событие"}`, string(dead[0].Value), "тело исходного сообщения не меняется")
	require.Equal(t, topic, headerValue(dead[0], kafkax.HeaderOriginalTopic))
	require.Contains(t, headerValue(dead[0], kafkax.HeaderError), "обработчик всегда падает")
	require.Equal(t, fmt.Sprint(cfg.MaxRetries), headerValue(dead[0], kafkax.HeaderAttempts))
	require.NotEmpty(t, headerValue(dead[0], kafkax.HeaderFailedAt))

	require.EqualValues(t, cfg.MaxRetries, attempts.Load(),
		"обработчик должен быть вызван ровно MaxRetries раз")

	cancel()
	require.NoError(t, <-done)
}

func TestКонсьюмерУспеваетПослеНесколькихПовторов(t *testing.T) {
	cfg := testsupport.RequireKafka(t)
	topic := testsupport.NewTopic(t, cfg, "test.recover")

	producer := kafkax.NewProducer(cfg, testsupport.Logger())
	t.Cleanup(func() { _ = producer.Close() })

	orderID := uuid.NewString()
	require.NoError(t, producer.Publish(context.Background(), kafkax.Message{
		Topic: topic, Key: orderID, Value: []byte(`{"ok":true}`),
	}))

	var attempts atomic.Int32
	processed := make(chan struct{}, 1)

	handler := func(ctx context.Context, msg kafkax.Inbound) error {
		if attempts.Add(1) < 2 {
			return errors.New("временная ошибка базы")
		}
		select {
		case processed <- struct{}{}:
		default:
		}
		return nil
	}

	consumer, err := kafkax.NewConsumer(cfg, kafkax.ConsumerOptions{
		Topics:  []string{topic},
		GroupID: "test-recover-" + uuid.NewString()[:8],
	}, handler, producer, testsupport.Logger())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	select {
	case <-processed:
	case <-time.After(45 * time.Second):
		t.Fatal("сообщение не было обработано после повторов")
	}

	require.EqualValues(t, 2, attempts.Load())

	cancel()
	require.NoError(t, <-done)
}

func TestНеповторяемаяОшибкаУходитВDLQСразу(t *testing.T) {
	cfg := testsupport.RequireKafka(t)
	topic := testsupport.NewTopic(t, cfg, "test.nonretryable")

	producer := kafkax.NewProducer(cfg, testsupport.Logger())
	t.Cleanup(func() { _ = producer.Close() })

	require.NoError(t, producer.Publish(context.Background(), kafkax.Message{
		Topic: topic, Key: uuid.NewString(), Value: []byte(`{"broken":`),
	}))

	var attempts atomic.Int32
	handler := func(ctx context.Context, msg kafkax.Inbound) error {
		attempts.Add(1)
		return kafkax.NonRetryable(errors.New("битый JSON"))
	}

	consumer, err := kafkax.NewConsumer(cfg, kafkax.ConsumerOptions{
		Topics:  []string{topic},
		GroupID: "test-nonretryable-" + uuid.NewString()[:8],
	}, handler, producer, testsupport.Logger())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	dead := readMessages(t, cfg, topic+cfg.DLQSuffix, 1, 45*time.Second)
	require.Contains(t, headerValue(dead[0], kafkax.HeaderError), "битый JSON")
	require.EqualValues(t, 1, attempts.Load(), "неповторяемая ошибка не должна приводить к повторам")

	cancel()
	require.NoError(t, <-done)
}

func TestСообщенияОдногоЗаказаПопадаютВОднуПартицию(t *testing.T) {
	cfg := testsupport.RequireKafka(t)

	// Топик с тремя партициями — как в рабочем стенде.
	topic := "test.partitioning." + uuid.NewString()[:8]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.NoError(t, kafkax.EnsureTopics(ctx, cfg, []kafkax.TopicSpec{
		{Name: topic, Partitions: 3, ReplicationFactor: 1},
	}, testsupport.Logger()))

	producer := kafkax.NewProducer(cfg, testsupport.Logger())
	t.Cleanup(func() { _ = producer.Close() })

	orderID := uuid.NewString()
	msgs := make([]kafkax.Message, 0, 6)
	for i := range 6 {
		msgs = append(msgs, kafkax.Message{
			Topic: topic,
			Key:   orderID,
			Value: []byte(fmt.Sprintf(`{"n":%d}`, i)),
		})
	}
	require.NoError(t, producer.Publish(context.Background(), msgs...))

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		GroupID:  "test-partitioning-" + uuid.NewString()[:8],
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 1 << 20,
		MaxWait:  200 * time.Millisecond,
	})
	t.Cleanup(func() { _ = reader.Close() })

	readCtx, readCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer readCancel()

	partitions := make(map[int]struct{})
	var order []string
	for range 6 {
		msg, err := reader.ReadMessage(readCtx)
		require.NoError(t, err)
		partitions[msg.Partition] = struct{}{}
		order = append(order, string(msg.Value))
	}

	require.Len(t, partitions, 1, "все события одного заказа должны попасть в одну партицию")
	require.Equal(t, []string{`{"n":0}`, `{"n":1}`, `{"n":2}`, `{"n":3}`, `{"n":4}`, `{"n":5}`}, order,
		"порядок событий одного заказа должен сохраняться")
}
