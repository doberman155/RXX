//go:build integration

package user

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"

	"github.com/hshsb/shop/internal/testsupport"
	"github.com/hshsb/shop/migrations"
	"github.com/hshsb/shop/pkg/auth"
	"github.com/hshsb/shop/pkg/config"
)

type fixture struct {
	repo    *Repository
	svc     *Service
	handler http.Handler
	tokens  *auth.Manager
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	pool := testsupport.NewDatabase(t, migrations.DirUser)
	repo := NewRepository(pool)

	tokens, err := auth.NewManager("секрет-интеграционных-тестов", time.Hour, "user-service")
	require.NoError(t, err)

	// MinCost ускоряет тесты: проверяется логика, а не стойкость хеша.
	svc := NewService(repo, tokens, bcrypt.MinCost, testsupport.Logger())
	handler := buildHandler(config.App{ServiceName: ServiceName}, svc, tokens, repo, testsupport.Logger())

	return &fixture{repo: repo, svc: svc, handler: handler, tokens: tokens}
}

func (f *fixture) do(t *testing.T, method, path, body, token string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	return rec
}

func TestИнтеграцияРегистрацияИВход(t *testing.T) {
	f := newFixture(t)
	body := `{"email":"User@Shop.Local","password":"надёжный_пароль"}`

	var created userResponse
	t.Run("регистрация создаёт пользователя с ролью USER", func(t *testing.T) {
		rec := f.do(t, http.MethodPost, "/api/v1/auth/register", body, "")
		require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))

		require.Equal(t, auth.RoleUser, created.Role)
		require.Equal(t, "user@shop.local", created.Email, "e-mail приводится к нижнему регистру")
		require.NotContains(t, rec.Body.String(), "password", "хеш пароля наружу не отдаётся")
	})

	t.Run("повторная регистрация отклоняется", func(t *testing.T) {
		rec := f.do(t, http.MethodPost, "/api/v1/auth/register", body, "")
		require.Equal(t, http.StatusConflict, rec.Code)
	})

	t.Run("регистрация с другим регистром e-mail отклоняется", func(t *testing.T) {
		rec := f.do(t, http.MethodPost, "/api/v1/auth/register",
			`{"email":"USER@shop.local","password":"надёжный_пароль"}`, "")
		require.Equal(t, http.StatusConflict, rec.Code)
	})

	t.Run("пароль хранится только в виде bcrypt-хеша", func(t *testing.T) {
		stored, err := f.repo.GetByEmail(context.Background(), "user@shop.local")
		require.NoError(t, err)
		require.NotEqual(t, "надёжный_пароль", stored.PasswordHash)
		require.NoError(t, bcrypt.CompareHashAndPassword([]byte(stored.PasswordHash), []byte("надёжный_пароль")))
	})

	var token string
	t.Run("вход выдаёт токен", func(t *testing.T) {
		rec := f.do(t, http.MethodPost, "/api/v1/auth/login", body, "")
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var issued tokenResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &issued))
		require.Equal(t, "Bearer", issued.TokenType)
		require.Equal(t, created.ID, issued.UserID)
		require.True(t, issued.ExpiresAt.After(time.Now()))

		claims, err := f.tokens.Parse(issued.AccessToken)
		require.NoError(t, err)
		require.Equal(t, auth.RoleUser, claims.Role)

		token = issued.AccessToken
	})

	t.Run("неверный пароль отклоняется", func(t *testing.T) {
		rec := f.do(t, http.MethodPost, "/api/v1/auth/login",
			`{"email":"user@shop.local","password":"неверный_пароль"}`, "")
		require.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("несуществующий пользователь отклоняется так же", func(t *testing.T) {
		rec := f.do(t, http.MethodPost, "/api/v1/auth/login",
			`{"email":"нет@shop.local","password":"надёжный_пароль"}`, "")
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		require.Contains(t, rec.Body.String(), ErrInvalidCredentials.Error())
	})

	t.Run("профиль доступен по токену", func(t *testing.T) {
		rec := f.do(t, http.MethodGet, "/api/v1/users/me", "", token)
		require.Equal(t, http.StatusOK, rec.Code)

		var me userResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &me))
		require.Equal(t, created.ID, me.ID)
	})

	t.Run("профиль без токена недоступен", func(t *testing.T) {
		require.Equal(t, http.StatusUnauthorized, f.do(t, http.MethodGet, "/api/v1/users/me", "", "").Code)
	})

	t.Run("список пользователей доступен только ADMIN", func(t *testing.T) {
		require.Equal(t, http.StatusForbidden, f.do(t, http.MethodGet, "/api/v1/users", "", token).Code)
	})
}

func TestИнтеграцияВалидацияРегистрации(t *testing.T) {
	f := newFixture(t)

	tests := []struct {
		name string
		body string
		code int
	}{
		{"пустое тело", ``, http.StatusBadRequest},
		{"некорректный e-mail", `{"email":"не-почта","password":"надёжный_пароль"}`, http.StatusBadRequest},
		{"короткий пароль", `{"email":"a@shop.local","password":"корот"}`, http.StatusBadRequest},
		{"лишнее поле", `{"email":"a@shop.local","password":"надёжный_пароль","role":"ADMIN"}`, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := f.do(t, http.MethodPost, "/api/v1/auth/register", tt.body, "")
			require.Equal(t, tt.code, rec.Code, rec.Body.String())
		})
	}
}

func TestИнтеграцияРольЧерезТелоЗапросаНеНазначается(t *testing.T) {
	f := newFixture(t)

	// Поле role в теле неизвестно обработчику — запрос отклоняется целиком,
	// поэтому роль ADMIN нельзя получить самостоятельно.
	rec := f.do(t, http.MethodPost, "/api/v1/auth/register",
		`{"email":"hacker@shop.local","password":"надёжный_пароль","role":"ADMIN"}`, "")
	require.Equal(t, http.StatusBadRequest, rec.Code)

	_, err := f.repo.GetByEmail(context.Background(), "hacker@shop.local")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestИнтеграцияАдминистраторСтенда(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	require.NoError(t, f.svc.EnsureAdmin(ctx, "", ""), "без переменных окружения вызов ничего не делает")

	require.NoError(t, f.svc.EnsureAdmin(ctx, "admin@shop.local", "пароль_админа"))

	admin, err := f.repo.GetByEmail(ctx, "admin@shop.local")
	require.NoError(t, err)
	require.Equal(t, auth.RoleAdmin, admin.Role)

	t.Run("повторный вызов обновляет пароль, а не создаёт дубль", func(t *testing.T) {
		require.NoError(t, f.svc.EnsureAdmin(ctx, "admin@shop.local", "новый_пароль_админа"))

		updated, err := f.repo.GetByEmail(ctx, "admin@shop.local")
		require.NoError(t, err)
		require.Equal(t, admin.ID, updated.ID)
		require.NoError(t, bcrypt.CompareHashAndPassword([]byte(updated.PasswordHash), []byte("новый_пароль_админа")))
	})

	t.Run("администратор видит список пользователей", func(t *testing.T) {
		token, _, err := f.tokens.Issue(admin.ID.String(), admin.Email, auth.RoleAdmin)
		require.NoError(t, err)

		rec := f.do(t, http.MethodGet, "/api/v1/users?limit=10", "", token)
		require.Equal(t, http.StatusOK, rec.Code)

		var list listResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
		require.Equal(t, 1, list.Total)

		rec = f.do(t, http.MethodGet, "/api/v1/users/"+admin.ID.String(), "", token)
		require.Equal(t, http.StatusOK, rec.Code)

		rec = f.do(t, http.MethodGet, "/api/v1/users/"+uuid.NewString(), "", token)
		require.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("некорректные данные администратора отклоняются", func(t *testing.T) {
		require.Error(t, f.svc.EnsureAdmin(ctx, "не-почта", "пароль_админа"))
		require.Error(t, f.svc.EnsureAdmin(ctx, "admin@shop.local", "корот"))
	})
}

func TestИнтеграцияЗдоровьеСервиса(t *testing.T) {
	f := newFixture(t)

	require.Equal(t, http.StatusOK, f.do(t, http.MethodGet, "/healthz", "", "").Code)
	require.Equal(t, http.StatusOK, f.do(t, http.MethodGet, "/readyz", "", "").Code)
}
