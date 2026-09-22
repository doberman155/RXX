package user

import (
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/hshsb/shop/pkg/auth"
	"github.com/hshsb/shop/pkg/httpx"
)

// Handler — REST-слой user-service.
type Handler struct {
	svc *Service
}

// NewHandler создаёт обработчики.
func NewHandler(svc *Service) *Handler { return &Handler{svc: svc} }

// Register настраивает маршруты на ServeMux (шаблоны Go 1.22+).
// protected — middleware проверки JWT, admin — дополнительная проверка роли.
func (h *Handler) Register(mux *http.ServeMux, protected, admin httpx.Middleware) {
	mux.HandleFunc("POST /api/v1/auth/register", h.register)
	mux.HandleFunc("POST /api/v1/auth/login", h.login)

	mux.Handle("GET /api/v1/users/me", protected(http.HandlerFunc(h.me)))
	mux.Handle("GET /api/v1/users", protected(admin(http.HandlerFunc(h.list))))
	mux.Handle("GET /api/v1/users/{id}", protected(admin(http.HandlerFunc(h.getByID))))
}

type credentialsRequest struct {
	Email string `json:"email"`
	// Пароль не попадает ни в логи, ни в ответы.
	Password string `json:"password"`
}

type userResponse struct {
	ID        uuid.UUID `json:"id"`
	Email     string    `json:"email"`
	Role      auth.Role `json:"role"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type tokenResponse struct {
	AccessToken string    `json:"access_token"`
	TokenType   string    `json:"token_type"`
	ExpiresAt   time.Time `json:"expires_at"`
	UserID      uuid.UUID `json:"user_id"`
	Role        auth.Role `json:"role"`
}

type listResponse struct {
	Items  []userResponse `json:"items"`
	Total  int            `json:"total"`
	Limit  int            `json:"limit"`
	Offset int            `json:"offset"`
}

func toResponse(u User) userResponse {
	return userResponse{
		ID:        u.ID,
		Email:     u.Email,
		Role:      u.Role,
		CreatedAt: u.CreatedAt,
		UpdatedAt: u.UpdatedAt,
	}
}

func (h *Handler) register(w http.ResponseWriter, r *http.Request) {
	var req credentialsRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.Error(w, r, http.StatusBadRequest, httpx.CodeBadRequest, err.Error())
		return
	}

	created, err := h.svc.Register(r.Context(), req.Email, req.Password)
	var validationErr ValidationError
	switch {
	case errors.Is(err, ErrEmailAlreadyUsed):
		httpx.Error(w, r, http.StatusConflict, httpx.CodeConflict, err.Error())
		return
	case errors.As(err, &validationErr):
		httpx.ErrorWithFields(w, r, http.StatusBadRequest, httpx.CodeValidation, validationErr.Message,
			map[string]string{validationErr.Field: validationErr.Message})
		return
	case err != nil:
		httpx.FromRequest(r).Error("не удалось зарегистрировать пользователя", "error", err.Error())
		httpx.Error(w, r, http.StatusInternalServerError, httpx.CodeInternal, "внутренняя ошибка сервиса")
		return
	}

	httpx.JSON(w, r, http.StatusCreated, toResponse(created))
}

func (h *Handler) login(w http.ResponseWriter, r *http.Request) {
	var req credentialsRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.Error(w, r, http.StatusBadRequest, httpx.CodeBadRequest, err.Error())
		return
	}

	token, err := h.svc.Login(r.Context(), req.Email, req.Password)
	switch {
	case errors.Is(err, ErrInvalidCredentials):
		httpx.Error(w, r, http.StatusUnauthorized, httpx.CodeUnauthorized, ErrInvalidCredentials.Error())
		return
	case err != nil:
		httpx.FromRequest(r).Error("не удалось выполнить вход", "error", err.Error())
		httpx.Error(w, r, http.StatusInternalServerError, httpx.CodeInternal, "внутренняя ошибка сервиса")
		return
	}

	httpx.JSON(w, r, http.StatusOK, tokenResponse{
		AccessToken: token.AccessToken,
		TokenType:   "Bearer",
		ExpiresAt:   token.ExpiresAt,
		UserID:      token.UserID,
		Role:        token.Role,
	})
}

func (h *Handler) me(w http.ResponseWriter, r *http.Request) {
	claims := httpx.MustClaims(r)

	id, err := uuid.Parse(claims.UserID())
	if err != nil {
		httpx.Error(w, r, http.StatusUnauthorized, httpx.CodeUnauthorized, "некорректный идентификатор пользователя в токене")
		return
	}

	h.respondUser(w, r, id)
}

func (h *Handler) getByID(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpx.Error(w, r, http.StatusBadRequest, httpx.CodeBadRequest, "идентификатор пользователя должен быть UUID")
		return
	}
	h.respondUser(w, r, id)
}

func (h *Handler) respondUser(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	u, err := h.svc.GetByID(r.Context(), id)
	switch {
	case errors.Is(err, ErrNotFound):
		httpx.Error(w, r, http.StatusNotFound, httpx.CodeNotFound, ErrNotFound.Error())
		return
	case err != nil:
		httpx.FromRequest(r).Error("не удалось получить пользователя", "error", err.Error())
		httpx.Error(w, r, http.StatusInternalServerError, httpx.CodeInternal, "внутренняя ошибка сервиса")
		return
	}

	httpx.JSON(w, r, http.StatusOK, toResponse(u))
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	limit, err := httpx.QueryInt(r, "limit", 20, 1, 100)
	if err != nil {
		httpx.Error(w, r, http.StatusBadRequest, httpx.CodeValidation, err.Error())
		return
	}
	offset, err := httpx.QueryInt(r, "offset", 0, 0, 1_000_000)
	if err != nil {
		httpx.Error(w, r, http.StatusBadRequest, httpx.CodeValidation, err.Error())
		return
	}

	users, total, err := h.svc.List(r.Context(), limit, offset)
	if err != nil {
		httpx.FromRequest(r).Error("не удалось получить список пользователей", "error", err.Error())
		httpx.Error(w, r, http.StatusInternalServerError, httpx.CodeInternal, "внутренняя ошибка сервиса")
		return
	}

	items := make([]userResponse, 0, len(users))
	for _, u := range users {
		items = append(items, toResponse(u))
	}

	httpx.JSON(w, r, http.StatusOK, listResponse{Items: items, Total: total, Limit: limit, Offset: offset})
}
