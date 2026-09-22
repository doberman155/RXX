package user

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/hshsb/shop/pkg/auth"
	"github.com/hshsb/shop/pkg/postgres"
)

// Repository — доступ к таблице users.
type Repository struct {
	pool *postgres.Pool
}

// NewRepository создаёт репозиторий.
func NewRepository(pool *postgres.Pool) *Repository { return &Repository{pool: pool} }

const userColumns = `id, email, password_hash, role, created_at, updated_at`

// Create сохраняет нового пользователя.
// Возвращает ErrEmailAlreadyUsed при конфликте по e-mail.
func (r *Repository) Create(ctx context.Context, u User) (User, error) {
	const query = `
		INSERT INTO users (id, email, password_hash, role)
		VALUES ($1, $2, $3, $4)
		RETURNING ` + userColumns

	row := r.pool.QueryRow(ctx, query, u.ID, u.Email, u.PasswordHash, u.Role)

	created, err := scanUser(row)
	if err != nil {
		if postgres.IsUniqueViolation(err) {
			return User{}, ErrEmailAlreadyUsed
		}
		return User{}, fmt.Errorf("создать пользователя: %w", err)
	}
	return created, nil
}

// GetByEmail ищет пользователя по e-mail без учёта регистра.
func (r *Repository) GetByEmail(ctx context.Context, email string) (User, error) {
	const query = `SELECT ` + userColumns + ` FROM users WHERE lower(email) = lower($1)`

	u, err := scanUser(r.pool.QueryRow(ctx, query, email))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return User{}, ErrNotFound
		}
		return User{}, fmt.Errorf("получить пользователя по e-mail: %w", err)
	}
	return u, nil
}

// GetByID ищет пользователя по идентификатору.
func (r *Repository) GetByID(ctx context.Context, id uuid.UUID) (User, error) {
	const query = `SELECT ` + userColumns + ` FROM users WHERE id = $1`

	u, err := scanUser(r.pool.QueryRow(ctx, query, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return User{}, ErrNotFound
		}
		return User{}, fmt.Errorf("получить пользователя по id: %w", err)
	}
	return u, nil
}

// List возвращает страницу пользователей и их общее количество.
func (r *Repository) List(ctx context.Context, limit, offset int) ([]User, int, error) {
	const query = `
		SELECT ` + userColumns + `, count(*) OVER () AS total
		FROM users
		ORDER BY created_at DESC, id
		LIMIT $1 OFFSET $2`

	rows, err := r.pool.Query(ctx, query, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("получить список пользователей: %w", err)
	}
	defer rows.Close()

	var (
		users []User
		total int
	)
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Email, &u.PasswordHash, &u.Role, &u.CreatedAt, &u.UpdatedAt, &total); err != nil {
			return nil, 0, fmt.Errorf("прочитать пользователя: %w", err)
		}
		users = append(users, u)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("обойти список пользователей: %w", err)
	}
	return users, total, nil
}

// UpsertAdmin создаёт администратора или обновляет его пароль и роль.
// Используется для первичной настройки стенда.
func (r *Repository) UpsertAdmin(ctx context.Context, u User) (User, error) {
	const query = `
		INSERT INTO users (id, email, password_hash, role)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (lower(email)) DO UPDATE
		SET password_hash = EXCLUDED.password_hash,
		    role          = EXCLUDED.role,
		    updated_at    = now()
		RETURNING ` + userColumns

	admin, err := scanUser(r.pool.QueryRow(ctx, query, u.ID, u.Email, u.PasswordHash, auth.RoleAdmin))
	if err != nil {
		return User{}, fmt.Errorf("создать администратора: %w", err)
	}
	return admin, nil
}

// Ping используется readiness-пробой.
func (r *Repository) Ping(ctx context.Context) error { return r.pool.Ping(ctx) }

func scanUser(row pgx.Row) (User, error) {
	var u User
	if err := row.Scan(&u.ID, &u.Email, &u.PasswordHash, &u.Role, &u.CreatedAt, &u.UpdatedAt); err != nil {
		return User{}, err
	}
	return u, nil
}
