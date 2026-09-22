// Команда kafka-admin создаёт топики проекта (по 3 партиции) и их DLQ.
// Запускается make-целью topics и один раз при старте стека в docker compose.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/hshsb/shop/pkg/config"
	"github.com/hshsb/shop/pkg/kafkax"
	"github.com/hshsb/shop/pkg/logger"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "kafka-admin завершился с ошибкой: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Утилите нужны только брокеры, но переменные POSTGRES_DSN и JWT_SECRET
	// обязательны в общем загрузчике, поэтому подставляем безопасные заглушки.
	setIfEmpty("POSTGRES_DSN", "postgres://unused/unused")
	setIfEmpty("JWT_SECRET", "kafka-admin-does-not-use-jwt")

	cfg, err := config.Load("kafka-admin", "0")
	if err != nil {
		return err
	}

	log := logger.New(cfg.ServiceName, cfg.LogLevel)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if err := kafkax.WaitForBroker(ctx, cfg.Kafka, time.Minute, log); err != nil {
		return err
	}

	specs := kafkax.DesiredTopics(cfg.Kafka)
	if err := kafkax.EnsureTopics(ctx, cfg.Kafka, specs, log); err != nil {
		return err
	}

	log.Info("топики готовы", "count", len(specs), "partitions", cfg.Kafka.Partitions)
	return nil
}

func setIfEmpty(key, value string) {
	if os.Getenv(key) == "" {
		_ = os.Setenv(key, value)
	}
}
