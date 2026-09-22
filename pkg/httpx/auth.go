package httpx

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/hshsb/shop/pkg/auth"
	"github.com/hshsb/shop/pkg/logger"
)

type claimsKey struct{}

// Authenticate проверяет подпись JWT и кладёт claims в контекст запроса.
// Сам токен в логи не попадает.
func Authenticate(m *auth.Manager) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, ok := auth.BearerToken(r.Header.Get("Authorization"))
			if !ok {
				Error(w, r, http.StatusUnauthorized, CodeUnauthorized, "требуется заголовок Authorization: Bearer <token>")
				return
			}

			claims, err := m.Parse(raw)
			if err != nil {
				msg := "невалидный токен"
				if errors.Is(err, auth.ErrExpiredToken) {
					msg = "срок действия токена истёк"
				}
				FromRequest(r).Warn("отклонён запрос с некорректным токеном", slog.String("reason", err.Error()))
				Error(w, r, http.StatusUnauthorized, CodeUnauthorized, msg)
				return
			}

			ctx := WithClaims(r.Context(), claims)
			ctx = logger.Into(ctx, logger.From(ctx).With(
				slog.String(logger.KeyUserID, claims.UserID()),
				slog.String("role", claims.Role.String()),
			))
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireRole пропускает дальше только пользователей с одной из ролей.
// Используется после Authenticate.
func RequireRole(roles ...auth.Role) Middleware {
	allowed := make(map[auth.Role]struct{}, len(roles))
	for _, r := range roles {
		allowed[r] = struct{}{}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims, ok := ClaimsFrom(r.Context())
			if !ok {
				Error(w, r, http.StatusUnauthorized, CodeUnauthorized, "требуется аутентификация")
				return
			}
			if _, ok := allowed[claims.Role]; !ok {
				Error(w, r, http.StatusForbidden, CodeForbidden, "недостаточно прав для операции")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// WithClaims кладёт claims в контекст.
func WithClaims(ctx context.Context, claims *auth.Claims) context.Context {
	return context.WithValue(ctx, claimsKey{}, claims)
}

// ClaimsFrom достаёт claims из контекста запроса.
func ClaimsFrom(ctx context.Context) (*auth.Claims, bool) {
	claims, ok := ctx.Value(claimsKey{}).(*auth.Claims)
	return claims, ok && claims != nil
}

// MustClaims возвращает claims; вызывается только внутри защищённых маршрутов.
func MustClaims(r *http.Request) *auth.Claims {
	claims, ok := ClaimsFrom(r.Context())
	if !ok {
		panic("httpx: маршрут защищён, но claims отсутствуют в контексте")
	}
	return claims
}
