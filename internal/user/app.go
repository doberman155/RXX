package user

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/hshsb/shop/internal/platform"
	"github.com/hshsb/shop/migrations"
	"github.com/hshsb/shop/pkg/auth"
	"github.com/hshsb/shop/pkg/config"
	"github.com/hshsb/shop/pkg/httpx"
	"github.com/hshsb/shop/pkg/logger"
	"github.com/hshsb/shop/pkg/postgres"
)

// ServiceName — имя сервиса в логах и ответах /healthz.
const ServiceName = "user-service"

// Run поднимает user-service и блокируется до сигнала остановки.
// Kafka сервису не нужна: он общается с клиентами только по REST.
func Run(ctx context.Context) error {
	cfg, err := config.Load(ServiceName, "8081")
	if err != nil {
		return err
	}

	log := logger.New(cfg.ServiceName, cfg.LogLevel)
	slog.SetDefault(log)

	if cfg.Postgres.MigrateOnStart {
		if err := postgres.Migrate(ctx, cfg.Postgres.DSN, migrations.FS, migrations.DirUser, log); err != nil {
			return err
		}
	}

	pool, err := postgres.Connect(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	defer pool.Close()

	tokens, err := auth.NewManager(cfg.JWT.Secret, cfg.JWT.TTL, cfg.JWT.Issuer)
	if err != nil {
		return err
	}

	repo := NewRepository(pool)
	svc := NewService(repo, tokens, cfg.Service.BcryptCost, log)

	if err := svc.EnsureAdmin(ctx, cfg.Service.BootstrapAdminEmail, cfg.Service.BootstrapAdminPassword); err != nil {
		return fmt.Errorf("подготовить администратора: %w", err)
	}

	handler := buildHandler(cfg, svc, tokens, repo, log)

	srv := httpx.NewServer(httpx.ServerConfig{
		Addr:            cfg.HTTP.Addr(),
		ReadTimeout:     cfg.HTTP.ReadTimeout,
		WriteTimeout:    cfg.HTTP.WriteTimeout,
		IdleTimeout:     cfg.HTTP.IdleTimeout,
		ShutdownTimeout: cfg.ShutdownTimeout,
	}, handler, log)

	return platform.Run(ctx, log, cfg.ShutdownTimeout,
		platform.Component{Name: "http", Run: srv.Run},
	)
}

// buildHandler собирает маршруты и middleware. Вынесен отдельно,
// чтобы его можно было использовать в интеграционных тестах.
func buildHandler(cfg config.App, svc *Service, tokens *auth.Manager, repo *Repository, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", httpx.Health(cfg.ServiceName))
	mux.HandleFunc("GET /readyz", httpx.Ready(cfg.ServiceName, repo.Ping))

	NewHandler(svc).Register(mux,
		httpx.Authenticate(tokens),
		httpx.RequireRole(auth.RoleAdmin),
	)

	return httpx.Chain(mux,
		httpx.RequestID(),
		httpx.Logging(log),
		httpx.Recovery(),
	)
}
