package user

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"github.com/hshsb/shop/pkg/auth"
	"github.com/hshsb/shop/pkg/logger"
)

// Service — бизнес-логика user-service.
type Service struct {
	repo       *Repository
	tokens     *auth.Manager
	bcryptCost int
	log        *slog.Logger
}

// NewService собирает сервис.
func NewService(repo *Repository, tokens *auth.Manager, bcryptCost int, log *slog.Logger) *Service {
	if bcryptCost < bcrypt.MinCost || bcryptCost > bcrypt.MaxCost {
		bcryptCost = bcrypt.DefaultCost
	}
	return &Service{repo: repo, tokens: tokens, bcryptCost: bcryptCost, log: log}
}

// Register создаёт пользователя с ролью USER.
// Пароль сохраняется только в виде bcrypt-хеша и нигде не логируется.
func (s *Service) Register(ctx context.Context, email, password string) (User, error) {
	if err := ValidateEmail(email); err != nil {
		return User{}, err
	}
	if err := ValidatePassword(password); err != nil {
		return User{}, err
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), s.bcryptCost)
	if err != nil {
		return User{}, fmt.Errorf("захешировать пароль: %w", err)
	}

	created, err := s.repo.Create(ctx, User{
		ID:           uuid.New(),
		Email:        NormalizeEmail(email),
		PasswordHash: string(hash),
		Role:         auth.RoleUser,
	})
	if err != nil {
		return User{}, err
	}

	logger.From(ctx).Info("пользователь зарегистрирован",
		slog.String(logger.KeyUserID, created.ID.String()),
		slog.String("role", created.Role.String()),
	)
	return created, nil
}

// Login проверяет учётные данные и выпускает JWT.
// Для несуществующего пользователя и неверного пароля возвращается одна и
// та же ошибка, чтобы не раскрывать наличие учётной записи.
func (s *Service) Login(ctx context.Context, email, password string) (Token, error) {
	u, err := s.repo.GetByEmail(ctx, NormalizeEmail(email))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			// Считаем фиктивный хеш, чтобы время ответа не зависело от
			// существования пользователя.
			_ = bcrypt.CompareHashAndPassword([]byte(dummyHash), []byte(password))
			return Token{}, ErrInvalidCredentials
		}
		return Token{}, err
	}

	if err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)); err != nil {
		return Token{}, ErrInvalidCredentials
	}

	token, expiresAt, err := s.tokens.Issue(u.ID.String(), u.Email, u.Role)
	if err != nil {
		return Token{}, err
	}

	logger.From(ctx).Info("выполнен вход", slog.String(logger.KeyUserID, u.ID.String()))

	return Token{AccessToken: token, ExpiresAt: expiresAt, UserID: u.ID, Role: u.Role}, nil
}

// GetByID возвращает пользователя по идентификатору.
func (s *Service) GetByID(ctx context.Context, id uuid.UUID) (User, error) {
	return s.repo.GetByID(ctx, id)
}

// List возвращает страницу пользователей (только для ADMIN).
func (s *Service) List(ctx context.Context, limit, offset int) ([]User, int, error) {
	return s.repo.List(ctx, limit, offset)
}

// EnsureAdmin создаёт администратора стенда, если он задан в переменных
// окружения. Регистрация через REST всегда выдаёт роль USER, поэтому
// первый ADMIN появляется только так.
func (s *Service) EnsureAdmin(ctx context.Context, email, password string) error {
	if email == "" && password == "" {
		return nil
	}
	if err := ValidateEmail(email); err != nil {
		return fmt.Errorf("BOOTSTRAP_ADMIN_EMAIL: %w", err)
	}
	if err := ValidatePassword(password); err != nil {
		return fmt.Errorf("BOOTSTRAP_ADMIN_PASSWORD: %w", err)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), s.bcryptCost)
	if err != nil {
		return fmt.Errorf("захешировать пароль администратора: %w", err)
	}

	admin, err := s.repo.UpsertAdmin(ctx, User{
		ID:           uuid.New(),
		Email:        NormalizeEmail(email),
		PasswordHash: string(hash),
		Role:         auth.RoleAdmin,
	})
	if err != nil {
		return err
	}

	s.log.Info("администратор стенда готов", slog.String(logger.KeyUserID, admin.ID.String()))
	return nil
}

// dummyHash — валидный bcrypt-хеш несуществующего пароля.
const dummyHash = "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"
