package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hshsb/shop/pkg/config"
)

// setMinimalEnv задаёт обязательные переменные окружения.
func setMinimalEnv(t *testing.T) {
	t.Helper()
	t.Setenv("POSTGRES_DSN", "postgres://shop:secret@localhost:5432/shop_order?sslmode=disable")
	t.Setenv("JWT_SECRET", "достаточно-длинный-секрет")
}

func TestLoadПодставляетЗначенияПоУмолчанию(t *testing.T) {
	setMinimalEnv(t)

	cfg, err := config.Load("order-service", "8083")
	require.NoError(t, err)

	require.Equal(t, "order-service", cfg.ServiceName)
	require.Equal(t, "8083", cfg.HTTP.Port)
	require.Equal(t, ":8083", cfg.HTTP.Addr())
	require.Equal(t, "info", cfg.LogLevel)
	require.Equal(t, 20*time.Second, cfg.ShutdownTimeout)
	require.Equal(t, []string{"localhost:9092"}, cfg.Kafka.Brokers)
	require.Equal(t, "order-service", cfg.Kafka.GroupID)
	require.Equal(t, 3, cfg.Kafka.Partitions)
	require.Equal(t, ".dlq", cfg.Kafka.DLQSuffix)
	require.Equal(t, 24*time.Hour, cfg.JWT.TTL)
	require.True(t, cfg.Postgres.MigrateOnStart)
	require.True(t, cfg.Service.AutoComplete)
}

func TestLoadЧитаетПереопределения(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("HTTP_PORT", "9999")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("KAFKA_BROKERS", " redpanda:9092 , redpanda-2:9092 ")
	t.Setenv("KAFKA_MAX_RETRIES", "5")
	t.Setenv("KAFKA_RETRY_BACKOFF", "250ms")
	t.Setenv("ORDER_AUTO_COMPLETE", "false")
	t.Setenv("POSTGRES_MIGRATE_ON_START", "false")
	t.Setenv("OUTBOX_BATCH_SIZE", "42")

	cfg, err := config.Load("order-service", "8083")
	require.NoError(t, err)

	require.Equal(t, "9999", cfg.HTTP.Port)
	require.Equal(t, "debug", cfg.LogLevel)
	require.Equal(t, []string{"redpanda:9092", "redpanda-2:9092"}, cfg.Kafka.Brokers)
	require.Equal(t, 5, cfg.Kafka.MaxRetries)
	require.Equal(t, 250*time.Millisecond, cfg.Kafka.RetryBackoff)
	require.False(t, cfg.Service.AutoComplete)
	require.False(t, cfg.Postgres.MigrateOnStart)
	require.Equal(t, 42, cfg.Outbox.BatchSize)
}

func TestLoadТребуетОбязательныеПеременные(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "")
	t.Setenv("JWT_SECRET", "")

	_, err := config.Load("order-service", "8083")
	require.Error(t, err)
	require.Contains(t, err.Error(), "POSTGRES_DSN")
	require.Contains(t, err.Error(), "JWT_SECRET")
}

func TestLoadОтклоняетКороткийСекрет(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://localhost/shop")
	t.Setenv("JWT_SECRET", "коротко")

	_, err := config.Load("order-service", "8083")
	require.Error(t, err)
	require.Contains(t, err.Error(), "JWT_SECRET")
}

func TestLoadСообщаетОбоВсехОшибкахСразу(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("HTTP_READ_TIMEOUT", "десять секунд")
	t.Setenv("KAFKA_MAX_RETRIES", "много")
	t.Setenv("POSTGRES_MAX_CONNS", "-1")

	_, err := config.Load("order-service", "8083")
	require.Error(t, err)
	require.Contains(t, err.Error(), "HTTP_READ_TIMEOUT")
	require.Contains(t, err.Error(), "KAFKA_MAX_RETRIES")
	require.Contains(t, err.Error(), "POSTGRES_MAX_CONNS")
}

func TestLoadОтклоняетНеположительнуюДлительность(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("OUTBOX_POLL_INTERVAL", "0s")

	_, err := config.Load("order-service", "8083")
	require.Error(t, err)
	require.Contains(t, err.Error(), "OUTBOX_POLL_INTERVAL")
}

func TestLoadОтклоняетНекорректныйBool(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("ORDER_AUTO_COMPLETE", "ага")

	_, err := config.Load("order-service", "8083")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ORDER_AUTO_COMPLETE")
}
