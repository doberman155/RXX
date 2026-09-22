// Package user реализует user-service: регистрацию, вход, выпуск JWT и роли.
package user

import (
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/hshsb/shop/pkg/auth"
)

// Ошибки предметной области.
var (
	ErrEmailAlreadyUsed   = errors.New("пользователь с таким e-mail уже существует")
	ErrInvalidCredentials = errors.New("неверный e-mail или пароль")
	ErrNotFound           = errors.New("пользователь не найден")
)

// ValidationError — ошибка входных данных; REST-слой отдаёт её как 400.
type ValidationError struct {
	Field   string
	Message string
}

// Error реализует интерфейс error.
func (e ValidationError) Error() string { return e.Message }

func invalid(field, format string, args ...any) error {
	return ValidationError{Field: field, Message: fmt.Sprintf(format, args...)}
}

// Ограничения на пароль.
const (
	// MinPasswordLen считается в символах: для пользователя «8 символов»
	// не должно зависеть от того, кириллица это или латиница.
	MinPasswordLen = 8
	// MaxPasswordLenBytes — ограничение bcrypt: всё после 72 байт молча
	// отбрасывается, поэтому длинные пароли отклоняем явно.
	MaxPasswordLenBytes = 72
)

// User — пользователь системы. PasswordHash наружу не отдаётся.
type User struct {
	ID           uuid.UUID
	Email        string
	PasswordHash string
	Role         auth.Role
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// Token — результат успешного входа.
type Token struct {
	AccessToken string
	ExpiresAt   time.Time
	UserID      uuid.UUID
	Role        auth.Role
}

// NormalizeEmail приводит e-mail к каноническому виду для хранения и поиска.
func NormalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

// ValidateEmail проверяет формат адреса.
func ValidateEmail(email string) error {
	email = NormalizeEmail(email)
	if email == "" {
		return invalid("email", "e-mail обязателен")
	}
	if len(email) > 254 {
		return invalid("email", "e-mail длиннее 254 символов")
	}
	if _, err := mail.ParseAddress(email); err != nil {
		return invalid("email", "некорректный формат e-mail")
	}
	return nil
}

// ValidatePassword проверяет требования к паролю.
func ValidatePassword(password string) error {
	switch {
	case password == "":
		return invalid("password", "пароль обязателен")
	case utf8.RuneCountInString(password) < MinPasswordLen:
		return invalid("password", "пароль короче %d символов", MinPasswordLen)
	case len(password) > MaxPasswordLenBytes:
		return invalid("password", "пароль длиннее %d байт", MaxPasswordLenBytes)
	default:
		return nil
	}
}
