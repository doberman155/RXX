package httpx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hshsb/shop/pkg/auth"
	"github.com/hshsb/shop/pkg/httpx"
	"github.com/hshsb/shop/pkg/logger"
)

// errDependency имитирует недоступную зависимость в readiness-пробе.
var errDependency = errors.New("зависимость недоступна")

func TestJSONПишетТелоИЗаголовок(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	httpx.JSON(rec, req, http.StatusCreated, map[string]string{"status": "ok"})

	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))
	require.JSONEq(t, `{"status":"ok"}`, rec.Body.String())
}

func TestNoContentНеПишетТело(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	httpx.NoContent(rec, httptest.NewRequest(http.MethodDelete, "/", nil))

	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Empty(t, rec.Body.String())
}

func TestErrorВозвращаетЕдиныйФормат(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	httpx.ErrorWithFields(rec, req, http.StatusBadRequest, httpx.CodeValidation, "плохой запрос",
		map[string]string{"email": "некорректный формат"})

	var body httpx.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, httpx.CodeValidation, body.Error.Code)
	require.Equal(t, "плохой запрос", body.Error.Message)
	require.Equal(t, "некорректный формат", body.Error.Fields["email"])
}

func TestDecodeJSON(t *testing.T) {
	t.Parallel()

	type payload struct {
		Name string `json:"name"`
	}

	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{"корректное тело", `{"name":"кофемолка"}`, ""},
		{"пустое тело", ``, "пустое"},
		{"битый json", `{"name":`, "некорректный JSON"},
		{"неизвестное поле", `{"name":"x","extra":1}`, "неизвестное поле"},
		{"неверный тип", `{"name":42}`, "неверный тип"},
		{"два объекта", `{"name":"x"}{"name":"y"}`, "один JSON-объект"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))

			var dst payload
			err := httpx.DecodeJSON(rec, req, &dst)

			if tt.wantErr == "" {
				require.NoError(t, err)
				require.Equal(t, "кофемолка", dst.Name)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestDecodeJSONОграничиваетРазмерТела(t *testing.T) {
	t.Parallel()

	huge := bytes.NewReader([]byte(`{"name":"` + strings.Repeat("a", httpx.MaxBodyBytes+10) + `"}`))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", huge)

	var dst struct {
		Name string `json:"name"`
	}
	err := httpx.DecodeJSON(rec, req, &dst)
	require.Error(t, err)
}

func TestQueryInt(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/?limit=30&bad=x&big=999", nil)

	v, err := httpx.QueryInt(req, "limit", 20, 1, 100)
	require.NoError(t, err)
	require.Equal(t, 30, v)

	v, err = httpx.QueryInt(req, "missing", 20, 1, 100)
	require.NoError(t, err)
	require.Equal(t, 20, v)

	_, err = httpx.QueryInt(req, "bad", 20, 1, 100)
	require.Error(t, err)

	_, err = httpx.QueryInt(req, "big", 20, 1, 100)
	require.Error(t, err)
}

func TestRequestIDПроставляетсяИВозвращается(t *testing.T) {
	t.Parallel()

	var seen string
	h := httpx.Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = httpx.RequestIDFrom(r.Context())
	}), httpx.RequestID())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	require.NotEmpty(t, seen)
	require.Equal(t, seen, rec.Header().Get(httpx.HeaderRequestID))
}

func TestRequestIDСохраняетВходящийЗаголовок(t *testing.T) {
	t.Parallel()

	h := httpx.Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "внешний-id", httpx.RequestIDFrom(r.Context()))
	}), httpx.RequestID())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(httpx.HeaderRequestID, "внешний-id")

	h.ServeHTTP(httptest.NewRecorder(), req)
}

func TestRecoveryПревращаетПаникуВ500(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	log := logger.NewWithWriter(&buf, "test", "error")

	h := httpx.Chain(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("что-то пошло не так")
	}), httpx.RequestID(), httpx.Logging(log), httpx.Recovery())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Contains(t, rec.Body.String(), httpx.CodeInternal)
	require.Contains(t, buf.String(), "паника в обработчике")
}

func TestLoggingПишетСтатусИДлительность(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	log := logger.NewWithWriter(&buf, "test", "info")

	h := httpx.Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}), httpx.RequestID(), httpx.Logging(log))

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/orders", nil))

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry))
	require.Equal(t, float64(http.StatusTeapot), entry["status"])
	require.Equal(t, "/orders", entry["path"])
	require.NotEmpty(t, entry["request_id"])
}

func TestAuthenticate(t *testing.T) {
	t.Parallel()

	m, err := auth.NewManager("секрет-для-тестов-middleware", time.Hour, "user-service")
	require.NoError(t, err)

	token, _, err := m.Issue("11111111-1111-1111-1111-111111111111", "user@shop.local", auth.RoleUser)
	require.NoError(t, err)

	var buf bytes.Buffer
	protected := httpx.Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims := httpx.MustClaims(r)
		_, _ = io.WriteString(w, claims.UserID())
	}), httpx.RequestID(), httpx.Logging(logger.NewWithWriter(&buf, "test", "error")), httpx.Authenticate(m))

	t.Run("без заголовка 401", func(t *testing.T) {
		rec := httptest.NewRecorder()
		protected.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		require.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("битый токен 401", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer не-токен")

		rec := httptest.NewRecorder()
		protected.ServeHTTP(rec, req)
		require.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("валидный токен пропускается", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		rec := httptest.NewRecorder()
		protected.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "11111111-1111-1111-1111-111111111111", rec.Body.String())
	})

	t.Run("токен не попадает в логи", func(t *testing.T) {
		require.NotContains(t, buf.String(), token)
	})
}

func TestRequireRole(t *testing.T) {
	t.Parallel()

	m, err := auth.NewManager("секрет-для-тестов-ролей", time.Hour, "user-service")
	require.NoError(t, err)

	handler := httpx.Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), httpx.RequestID(), httpx.Authenticate(m), httpx.RequireRole(auth.RoleAdmin))

	for _, tt := range []struct {
		role auth.Role
		want int
	}{
		{auth.RoleAdmin, http.StatusOK},
		{auth.RoleUser, http.StatusForbidden},
	} {
		token, _, err := m.Issue("id-1", "user@shop.local", tt.role)
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.Equal(t, tt.want, rec.Code, "роль %s", tt.role)
	}
}

func TestRequireRoleБезАутентификации(t *testing.T) {
	t.Parallel()

	handler := httpx.Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), httpx.RequireRole(auth.RoleAdmin))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestHealthИReady(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	httpx.Health("test-service")(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "test-service")

	rec = httptest.NewRecorder()
	httpx.Ready("test-service", func(ctx context.Context) error { return nil })(
		rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	req = req.WithContext(logger.Into(req.Context(), logger.NewWithWriter(io.Discard, "test", "error")))
	httpx.Ready("test-service", func(ctx context.Context) error { return errDependency })(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
