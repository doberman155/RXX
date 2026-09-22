package auth_test

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"

	"github.com/hshsb/shop/pkg/auth"
)

const testSecret = "test-secret-not-used-anywhere-else"

func newManager(t *testing.T, ttl time.Duration) *auth.Manager {
	t.Helper()
	m, err := auth.NewManager(testSecret, ttl, "user-service")
	require.NoError(t, err)
	return m
}

func TestNewManagerОтклоняетНекорректныеПараметры(t *testing.T) {
	t.Parallel()

	_, err := auth.NewManager("", time.Hour, "user-service")
	require.Error(t, err)

	_, err = auth.NewManager(testSecret, 0, "user-service")
	require.Error(t, err)
}

func TestIssueИParseВозвращаютИсходныеДанные(t *testing.T) {
	t.Parallel()

	m := newManager(t, time.Hour)

	token, expiresAt, err := m.Issue("11111111-1111-1111-1111-111111111111", "user@shop.local", auth.RoleAdmin)
	require.NoError(t, err)
	require.NotEmpty(t, token)
	require.WithinDuration(t, time.Now().Add(time.Hour), expiresAt, 5*time.Second)

	claims, err := m.Parse(token)
	require.NoError(t, err)
	require.Equal(t, "11111111-1111-1111-1111-111111111111", claims.UserID())
	require.Equal(t, "user@shop.local", claims.Email)
	require.Equal(t, auth.RoleAdmin, claims.Role)
	require.Equal(t, "user-service", claims.Issuer)
}

func TestIssueОтклоняетНеизвестнуюРоль(t *testing.T) {
	t.Parallel()

	m := newManager(t, time.Hour)

	_, _, err := m.Issue("id", "user@shop.local", auth.Role("ROOT"))
	require.ErrorIs(t, err, auth.ErrUnknownRole)
}

func TestParseОтклоняетЧужуюПодпись(t *testing.T) {
	t.Parallel()

	issuer := newManager(t, time.Hour)
	token, _, err := issuer.Issue("id", "user@shop.local", auth.RoleUser)
	require.NoError(t, err)

	other, err := auth.NewManager("совершенно другой секрет", time.Hour, "user-service")
	require.NoError(t, err)

	_, err = other.Parse(token)
	require.ErrorIs(t, err, auth.ErrInvalidToken)
}

func TestParseОтклоняетПросроченныйТокен(t *testing.T) {
	t.Parallel()

	m := newManager(t, time.Millisecond)

	token, _, err := m.Issue("id", "user@shop.local", auth.RoleUser)
	require.NoError(t, err)

	time.Sleep(20 * time.Millisecond)

	_, err = m.Parse(token)
	require.ErrorIs(t, err, auth.ErrExpiredToken)
}

func TestParseОтклоняетАлгоритмNone(t *testing.T) {
	t.Parallel()

	// Классическая атака: подменить alg на none и убрать подпись.
	unsigned := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{
		"sub":  "id",
		"role": string(auth.RoleAdmin),
		"exp":  time.Now().Add(time.Hour).Unix(),
	})
	token, err := unsigned.SignedString(jwt.UnsafeAllowNoneSignatureType)
	require.NoError(t, err)

	_, err = newManager(t, time.Hour).Parse(token)
	require.ErrorIs(t, err, auth.ErrInvalidToken)
}

func TestParseОтклоняетТокенБезSubject(t *testing.T) {
	t.Parallel()

	raw := jwt.NewWithClaims(jwt.SigningMethodHS256, auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))},
		Role:             auth.RoleUser,
	})
	token, err := raw.SignedString([]byte(testSecret))
	require.NoError(t, err)

	_, err = newManager(t, time.Hour).Parse(token)
	require.ErrorIs(t, err, auth.ErrInvalidToken)
}

func TestParseОтклоняетНеизвестнуюРоль(t *testing.T) {
	t.Parallel()

	raw := jwt.NewWithClaims(jwt.SigningMethodHS256, auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "id",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
		Role: auth.Role("SUPERUSER"),
	})
	token, err := raw.SignedString([]byte(testSecret))
	require.NoError(t, err)

	_, err = newManager(t, time.Hour).Parse(token)
	require.ErrorIs(t, err, auth.ErrUnknownRole)
}

func TestBearerToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		header string
		want   string
		ok     bool
	}{
		{"обычный заголовок", "Bearer abc.def.ghi", "abc.def.ghi", true},
		{"регистр схемы не важен", "bearer abc", "abc", true},
		{"пустой заголовок", "", "", false},
		{"другая схема", "Basic abc", "", false},
		{"пустой токен", "Bearer    ", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := auth.BearerToken(tt.header)
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseRole(t *testing.T) {
	t.Parallel()

	role, err := auth.ParseRole(" admin ")
	require.NoError(t, err)
	require.Equal(t, auth.RoleAdmin, role)

	_, err = auth.ParseRole("owner")
	require.ErrorIs(t, err, auth.ErrUnknownRole)
}

func TestTTL(t *testing.T) {
	t.Parallel()
	require.Equal(t, 2*time.Hour, newManager(t, 2*time.Hour).TTL())
}
