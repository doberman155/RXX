// Package auth отвечает за выпуск и проверку JWT, а также за роли USER/ADMIN.
// Подпись — HMAC-SHA256 на общем секрете из переменных окружения.
package auth

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Role — роль пользователя внутри токена.
type Role string

const (
	RoleUser  Role = "USER"
	RoleAdmin Role = "ADMIN"
)

// Valid сообщает, известна ли роль системе.
func (r Role) Valid() bool { return r == RoleUser || r == RoleAdmin }

// String реализует fmt.Stringer.
func (r Role) String() string { return string(r) }

// ParseRole нормализует строковое представление роли.
func ParseRole(s string) (Role, error) {
	r := Role(strings.ToUpper(strings.TrimSpace(s)))
	if !r.Valid() {
		return "", fmt.Errorf("%w: %q", ErrUnknownRole, s)
	}
	return r, nil
}

// Ошибки пакета.
var (
	ErrInvalidToken = errors.New("невалидный токен")
	ErrExpiredToken = errors.New("срок действия токена истёк")
	ErrUnknownRole  = errors.New("неизвестная роль")
)

// Claims — полезная нагрузка токена. Subject содержит идентификатор пользователя.
type Claims struct {
	jwt.RegisteredClaims
	Email string `json:"email"`
	Role  Role   `json:"role"`
}

// UserID возвращает идентификатор пользователя из subject.
func (c Claims) UserID() string { return c.Subject }

// Manager выпускает и проверяет токены.
type Manager struct {
	secret []byte
	ttl    time.Duration
	issuer string
	now    func() time.Time
}

// NewManager создаёт менеджер токенов. Пустой секрет считается ошибкой
// конфигурации, поэтому проверяется здесь же.
func NewManager(secret string, ttl time.Duration, issuer string) (*Manager, error) {
	if strings.TrimSpace(secret) == "" {
		return nil, errors.New("jwt: пустой секрет")
	}
	if ttl <= 0 {
		return nil, errors.New("jwt: срок жизни токена должен быть положительным")
	}
	return &Manager{secret: []byte(secret), ttl: ttl, issuer: issuer, now: time.Now}, nil
}

// TTL возвращает настроенный срок жизни токена.
func (m *Manager) TTL() time.Duration { return m.ttl }

// Issue выпускает подписанный токен для пользователя.
func (m *Manager) Issue(userID, email string, role Role) (string, time.Time, error) {
	if !role.Valid() {
		return "", time.Time{}, fmt.Errorf("%w: %q", ErrUnknownRole, role)
	}
	now := m.now().UTC()
	expiresAt := now.Add(m.ttl)

	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			Issuer:    m.issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			ID:        newTokenID(now),
		},
		Email: email,
		Role:  role,
	}

	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(m.secret)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("подписать токен: %w", err)
	}
	return signed, expiresAt, nil
}

// Parse проверяет подпись и срок действия токена.
func (m *Manager) Parse(token string) (*Claims, error) {
	claims := &Claims{}
	parsed, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("%w: неожиданный алгоритм подписи %v", ErrInvalidToken, t.Header["alg"])
		}
		return m.secret, nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))

	switch {
	case errors.Is(err, jwt.ErrTokenExpired):
		return nil, ErrExpiredToken
	case err != nil:
		return nil, fmt.Errorf("%w: %s", ErrInvalidToken, err.Error())
	case !parsed.Valid:
		return nil, ErrInvalidToken
	}

	if claims.Subject == "" {
		return nil, fmt.Errorf("%w: пустой subject", ErrInvalidToken)
	}
	if !claims.Role.Valid() {
		return nil, fmt.Errorf("%w: %q", ErrUnknownRole, claims.Role)
	}
	return claims, nil
}

// BearerToken извлекает токен из заголовка Authorization.
func BearerToken(header string) (string, bool) {
	const prefix = "Bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", false
	}
	token := strings.TrimSpace(header[len(prefix):])
	if token == "" {
		return "", false
	}
	return token, true
}
