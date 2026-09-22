package notification

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/hshsb/shop/internal/platform"
	"github.com/hshsb/shop/migrations"
	"github.com/hshsb/shop/pkg/auth"
	"github.com/hshsb/shop/pkg/config"
	"github.com/hshsb/shop/pkg/events"
	"github.com/hshsb/shop/pkg/httpx"
	"github.com/hshsb/shop/pkg/kafkax"
	"github.com/hshsb/shop/pkg/logger"
	"github.com/hshsb/shop/pkg/postgres"
)

// ServiceName — имя сервиса в логах и ответах /healthz.
const ServiceName = "notification-service"

// ConsumerGroupSuffix отличает consumer-группу сервиса от групп других сервисов.
const ConsumerGroupSuffix = ".orders"

// Run поднимает notification-service: консьюмер событий заказа и HTTP для
// /healthz и просмотра уведомлений. Собственных событий сервис не публикует,
// поэтому outbox и релеер ему не нужны.
func Run(ctx context.Context) error {
	cfg, err := config.Load(ServiceName, "8084")
	if err != nil {
		return err
	}

	log := logger.New(cfg.ServiceName, cfg.LogLevel)
	slog.SetDefault(log)

	if cfg.Postgres.MigrateOnStart {
		if err := postgres.Migrate(ctx, cfg.Postgres.DSN, migrations.FS, migrations.DirNotification, log); err != nil {
			return err
		}
	}

	pool, err := postgres.Connect(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := kafkax.WaitForBroker(ctx, cfg.Kafka, time.Minute, log); err != nil {
		return err
	}

	tokens, err := auth.NewManager(cfg.JWT.Secret, cfg.JWT.TTL, cfg.JWT.Issuer)
	if err != nil {
		return err
	}

	// Продюсер нужен только для отправки в dead-letter topic.
	producer := kafkax.NewProducer(cfg.Kafka, log)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Error("не удалось закрыть продюсер", slog.String("error", err.Error()))
		}
	}()

	groupID := cfg.Kafka.GroupID + ConsumerGroupSuffix

	repo := NewRepository(pool)
	svc := NewService(repo, groupID, log)

	consumer, err := kafkax.NewConsumer(cfg.Kafka, kafkax.ConsumerOptions{
		Topics:  []string{events.TopicOrderCreated, events.TopicOrderStatusChanged},
		GroupID: groupID,
	}, EventHandler(svc), producer, log)
	if err != nil {
		return err
	}

	srv := httpx.NewServer(httpx.ServerConfig{
		Addr:            cfg.HTTP.Addr(),
		ReadTimeout:     cfg.HTTP.ReadTimeout,
		WriteTimeout:    cfg.HTTP.WriteTimeout,
		IdleTimeout:     cfg.HTTP.IdleTimeout,
		ShutdownTimeout: cfg.ShutdownTimeout,
	}, buildHandler(cfg, svc, tokens, repo, log), log)

	return platform.Run(ctx, log, cfg.ShutdownTimeout,
		platform.Component{Name: "http", Run: srv.Run},
		platform.Component{Name: "kafka-consumer", Run: consumer.Run},
	)
}

// buildHandler собирает маршруты и middleware.
func buildHandler(cfg config.App, svc *Service, tokens *auth.Manager, repo *Repository, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", httpx.Health(cfg.ServiceName))
	mux.HandleFunc("GET /readyz", httpx.Ready(cfg.ServiceName, repo.Ping, kafkax.Ping(cfg.Kafka)))

	NewHandler(svc).Register(mux, httpx.Authenticate(tokens))

	return httpx.Chain(mux,
		httpx.RequestID(),
		httpx.Logging(log),
		httpx.Recovery(),
	)
}
