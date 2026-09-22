package httpx

import (
	"context"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/google/uuid"

	"github.com/hshsb/shop/pkg/logger"
)

// Middleware — стандартная обёртка над http.Handler.
type Middleware func(http.Handler) http.Handler

// Chain применяет middleware в порядке объявления: первый в списке
// оказывается самым внешним.
func Chain(h http.Handler, mws ...Middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

type requestIDKey struct{}

// HeaderRequestID — заголовок сквозного идентификатора запроса.
const HeaderRequestID = "X-Request-Id"

// RequestID проставляет идентификатор запроса и кладёт его в контекст и заголовок ответа.
func RequestID() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get(HeaderRequestID)
			if id == "" {
				id = uuid.NewString()
			}
			w.Header().Set(HeaderRequestID, id)
			ctx := context.WithValue(r.Context(), requestIDKey{}, id)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequestIDFrom возвращает идентификатор запроса из контекста.
func RequestIDFrom(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey{}).(string); ok {
		return id
	}
	return ""
}

// FromRequest возвращает логгер, связанный с запросом.
func FromRequest(r *http.Request) *slog.Logger { return logger.From(r.Context()) }

// statusWriter запоминает код ответа и объём тела для логов.
type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
		w.ResponseWriter.WriteHeader(status)
	}
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}

// Logging логирует каждый запрос в структурированном виде.
// Тело запроса не логируется: в нём могут быть пароли.
func Logging(log *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			reqLog := log.With(
				slog.String(logger.KeyRequestID, RequestIDFrom(r.Context())),
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
			)
			ctx := logger.Into(r.Context(), reqLog)

			sw := &statusWriter{ResponseWriter: w}
			next.ServeHTTP(sw, r.WithContext(ctx))

			if sw.status == 0 {
				sw.status = http.StatusOK
			}

			attrs := []any{
				slog.Int("status", sw.status),
				slog.Int("bytes", sw.bytes),
				slog.Duration("duration", time.Since(start)),
			}
			switch {
			case sw.status >= http.StatusInternalServerError:
				reqLog.Error("http-запрос обработан", attrs...)
			case sw.status >= http.StatusBadRequest:
				reqLog.Warn("http-запрос обработан", attrs...)
			default:
				reqLog.Info("http-запрос обработан", attrs...)
			}
		})
	}
}

// Recovery превращает панику в 500, не роняя сервис.
func Recovery() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					FromRequest(r).Error("паника в обработчике",
						slog.Any("panic", rec),
						slog.String("stack", string(debug.Stack())),
					)
					Error(w, r, http.StatusInternalServerError, CodeInternal, "внутренняя ошибка сервиса")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
