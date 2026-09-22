// Package logger настраивает структурированное логирование (log/slog, JSON).
// Секреты (пароли, токены) в логи не пишутся никогда: за это отвечают
// вызывающие пакеты, которые не передают такие поля в логгер.
package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

// Ключи атрибутов, общие для всех сервисов.
const (
	KeyService   = "service"
	KeyOrderID   = "order_id"
	KeyEventID   = "event_id"
	KeyEventType = "event_type"
	KeyTopic     = "topic"
	KeyRequestID = "request_id"
	KeyUserID    = "user_id"
)

type ctxKey struct{}

// New создаёт JSON-логгер с уровнем level и атрибутом service.
func New(service, level string) *slog.Logger {
	return NewWithWriter(os.Stdout, service, level)
}

// NewWithWriter позволяет подменить вывод (используется в тестах).
func NewWithWriter(w io.Writer, service, level string) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: parseLevel(level)})
	return slog.New(h).With(slog.String(KeyService, service))
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Into кладёт логгер в контекст, чтобы нижние слои писали логи с теми же
// атрибутами (request_id, order_id), что и верхние.
func Into(ctx context.Context, log *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, log)
}

// From достаёт логгер из контекста; если его там нет — возвращает slog.Default().
func From(ctx context.Context) *slog.Logger {
	if log, ok := ctx.Value(ctxKey{}).(*slog.Logger); ok && log != nil {
		return log
	}
	return slog.Default()
}

// WithOrderID — короткий помощник: требование ТЗ, чтобы каждая запись,
// относящаяся к заказу, содержала его идентификатор.
func WithOrderID(log *slog.Logger, orderID string) *slog.Logger {
	return log.With(slog.String(KeyOrderID, orderID))
}
