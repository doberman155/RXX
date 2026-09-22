package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"log/slog"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // драйвер database/sql для goose
	"github.com/pressly/goose/v3"
)

// goose хранит диалект и базовую FS в глобальном состоянии, поэтому
// параллельные вызовы Migrate внутри одного процесса сериализуются.
var migrateMu sync.Mutex

// advisoryLockID — произвольный, но стабильный идентификатор блокировки,
// защищающей от одновременного накатывания миграций несколькими репликами.
const advisoryLockID int64 = 8_123_451

// Migrate применяет все миграции из встроенной файловой системы.
// dir — каталог внутри fsys (например "migrations/order").
func Migrate(ctx context.Context, dsn string, fsys fs.FS, dir string, log *slog.Logger) error {
	migrateMu.Lock()
	defer migrateMu.Unlock()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("открыть подключение для миграций: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Одно соединение удерживает advisory-lock, остальные нужны goose,
	// иначе пул исчерпается и миграции встанут в дедлок.
	db.SetMaxOpenConns(4)

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("настроить диалект goose: %w", err)
	}
	goose.SetBaseFS(fsys)
	goose.SetLogger(gooseLogger{log: log})

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("получить соединение для миграций: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", advisoryLockID); err != nil {
		return fmt.Errorf("взять advisory-lock миграций: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(unlockCtx, "SELECT pg_advisory_unlock($1)", advisoryLockID); err != nil {
			log.Warn("не удалось снять advisory-lock миграций", slog.String("error", err.Error()))
		}
	}()

	start := time.Now()
	if err := goose.UpContext(ctx, db, dir); err != nil {
		return fmt.Errorf("применить миграции из %s: %w", dir, err)
	}

	log.Info("миграции применены",
		slog.String("dir", dir),
		slog.Duration("duration", time.Since(start)),
	)
	return nil
}

// gooseLogger переводит вывод goose в структурированный лог сервиса.
type gooseLogger struct{ log *slog.Logger }

func (g gooseLogger) Printf(format string, v ...any) {
	g.log.Info("goose: " + trimNewline(fmt.Sprintf(format, v...)))
}

func (g gooseLogger) Fatalf(format string, v ...any) {
	g.log.Error("goose: " + trimNewline(fmt.Sprintf(format, v...)))
}

func trimNewline(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}
