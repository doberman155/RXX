//go:build integration

package testsupport

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/hshsb/shop/migrations"
	"github.com/hshsb/shop/pkg/config"
	"github.com/hshsb/shop/pkg/logger"
	"github.com/hshsb/shop/pkg/postgres"
)

// DefaultPostgresDSN указывает на PostgreSQL из docker-compose стенда.
const DefaultPostgresDSN = "postgres://shop:shop_local_password@localhost:5433/postgres?sslmode=disable"

// PostgresDSN возвращает строку подключения к серверу PostgreSQL.
// Переопределяется переменной TEST_POSTGRES_DSN (используется в CI).
func PostgresDSN() string {
	if dsn := strings.TrimSpace(os.Getenv("TEST_POSTGRES_DSN")); dsn != "" {
		return dsn
	}
	return DefaultPostgresDSN
}

// Logger возвращает молчаливый логгер для тестов.
func Logger() *slog.Logger { return logger.NewWithWriter(io.Discard, "test", "error") }

// NewDatabase создаёт отдельную базу под конкретный тест, применяет к ней
// миграции сервиса и возвращает пул соединений. База удаляется по завершении
// теста, поэтому тесты не влияют друг на друга и могут идти параллельно.
func NewDatabase(t testing.TB, migrationsDir string) *postgres.Pool {
	t.Helper()

	ctx := context.Background()
	baseDSN := PostgresDSN()

	admin, err := postgres.Connect(ctx, config.Postgres{
		DSN: baseDSN, MaxConns: 2, MinConns: 1, ConnectTimeout: 15 * time.Second,
	})
	if err != nil {
		t.Skipf("PostgreSQL недоступен (%v). Поднимите стенд: docker compose up -d postgres", err)
	}

	name := "test_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:24]

	if _, err := admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
		admin.Close()
		t.Fatalf("создать тестовую базу: %v", err)
	}

	dsn, err := withDatabase(baseDSN, name)
	if err != nil {
		admin.Close()
		t.Fatalf("собрать DSN тестовой базы: %v", err)
	}

	if err := postgres.Migrate(ctx, dsn, migrations.FS, migrationsDir, Logger()); err != nil {
		admin.Close()
		t.Fatalf("применить миграции: %v", err)
	}

	pool, err := postgres.Connect(ctx, config.Postgres{
		DSN: dsn, MaxConns: 6, MinConns: 1, ConnectTimeout: 15 * time.Second,
	})
	if err != nil {
		admin.Close()
		t.Fatalf("подключиться к тестовой базе: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		dropCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		if _, err := admin.Exec(dropCtx, fmt.Sprintf("DROP DATABASE IF EXISTS %q WITH (FORCE)", name)); err != nil {
			t.Logf("не удалось удалить тестовую базу %s: %v", name, err)
		}
		admin.Close()
	})

	return pool
}

// withDatabase подменяет имя базы в строке подключения.
func withDatabase(dsn, name string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("разобрать DSN: %w", err)
	}
	u.Path = "/" + name
	return u.String(), nil
}
