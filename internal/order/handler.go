package order

import (
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/hshsb/shop/pkg/httpx"
)

// HeaderIdempotencyKey — заголовок ключа идемпотентности создания заказа.
const HeaderIdempotencyKey = "Idempotency-Key"

// Handler — REST-слой order-service.
type Handler struct {
	svc *Service
}

// NewHandler создаёт обработчики.
func NewHandler(svc *Service) *Handler { return &Handler{svc: svc} }

// Register настраивает маршруты. Все они требуют аутентификации.
func (h *Handler) Register(mux *http.ServeMux, protected httpx.Middleware) {
	mux.Handle("POST /api/v1/orders", protected(http.HandlerFunc(h.create)))
	mux.Handle("GET /api/v1/orders", protected(http.HandlerFunc(h.list)))
	mux.Handle("GET /api/v1/orders/{id}", protected(http.HandlerFunc(h.get)))
	mux.Handle("POST /api/v1/orders/{id}/cancel", protected(http.HandlerFunc(h.cancel)))
	mux.Handle("POST /api/v1/orders/{id}/complete", protected(http.HandlerFunc(h.complete)))
}

type createRequest struct {
	Items []CreateItem `json:"items"`
}

type cancelRequest struct {
	Reason string `json:"reason"`
}

type itemResponse struct {
	ProductID      uuid.UUID `json:"product_id"`
	ProductName    string    `json:"product_name,omitempty"`
	Quantity       int       `json:"quantity"`
	UnitPriceCents int64     `json:"unit_price_cents"`
}

type orderResponse struct {
	ID               uuid.UUID      `json:"id"`
	UserID           uuid.UUID      `json:"user_id"`
	Status           Status         `json:"status"`
	TotalAmountCents int64          `json:"total_amount_cents"`
	Currency         string         `json:"currency"`
	CancelReason     string         `json:"cancel_reason,omitempty"`
	Items            []itemResponse `json:"items"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
}

type listResponse struct {
	Items  []orderResponse `json:"items"`
	Total  int             `json:"total"`
	Limit  int             `json:"limit"`
	Offset int             `json:"offset"`
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	actor, ok := actorFrom(w, r)
	if !ok {
		return
	}

	var req createRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.Error(w, r, http.StatusBadRequest, httpx.CodeBadRequest, err.Error())
		return
	}

	created, existing, err := h.svc.Create(r.Context(), CreateInput{
		Actor:          actor,
		Items:          req.Items,
		IdempotencyKey: r.Header.Get(HeaderIdempotencyKey),
	})
	if err != nil {
		writeError(w, r, err, "не удалось создать заказ")
		return
	}

	// Повторный запрос с тем же ключом возвращает существующий заказ (200),
	// а не создаёт новый (201).
	status := http.StatusCreated
	if existing {
		status = http.StatusOK
	}
	httpx.JSON(w, r, status, toResponse(created))
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	actor, ok := actorFrom(w, r)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}

	o, err := h.svc.Get(r.Context(), actor, id)
	if err != nil {
		writeError(w, r, err, "не удалось получить заказ")
		return
	}

	httpx.JSON(w, r, http.StatusOK, toResponse(o))
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	actor, ok := actorFrom(w, r)
	if !ok {
		return
	}

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

	orders, total, err := h.svc.List(r.Context(), actor, Status(r.URL.Query().Get("status")), limit, offset)
	if err != nil {
		writeError(w, r, err, "не удалось получить список заказов")
		return
	}

	items := make([]orderResponse, 0, len(orders))
	for _, o := range orders {
		items = append(items, toResponse(o))
	}

	httpx.JSON(w, r, http.StatusOK, listResponse{Items: items, Total: total, Limit: limit, Offset: offset})
}

func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) {
	actor, ok := actorFrom(w, r)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}

	var req cancelRequest
	if r.ContentLength > 0 {
		if err := httpx.DecodeJSON(w, r, &req); err != nil {
			httpx.Error(w, r, http.StatusBadRequest, httpx.CodeBadRequest, err.Error())
			return
		}
	}

	o, err := h.svc.Cancel(r.Context(), actor, id, req.Reason)
	if err != nil {
		writeError(w, r, err, "не удалось отменить заказ")
		return
	}

	httpx.JSON(w, r, http.StatusOK, toResponse(o))
}

func (h *Handler) complete(w http.ResponseWriter, r *http.Request) {
	actor, ok := actorFrom(w, r)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}

	o, err := h.svc.Complete(r.Context(), actor, id)
	if err != nil {
		writeError(w, r, err, "не удалось завершить заказ")
		return
	}

	httpx.JSON(w, r, http.StatusOK, toResponse(o))
}

func actorFrom(w http.ResponseWriter, r *http.Request) (Actor, bool) {
	claims := httpx.MustClaims(r)

	userID, err := uuid.Parse(claims.UserID())
	if err != nil {
		httpx.Error(w, r, http.StatusUnauthorized, httpx.CodeUnauthorized, "некорректный идентификатор пользователя в токене")
		return Actor{}, false
	}
	return Actor{UserID: userID, Role: claims.Role}, true
}

func pathUUID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpx.Error(w, r, http.StatusBadRequest, httpx.CodeBadRequest, "идентификатор заказа должен быть UUID")
		return uuid.Nil, false
	}
	return id, true
}

func writeError(w http.ResponseWriter, r *http.Request, err error, logMessage string) {
	var (
		validationErr ValidationError
		transitionErr ErrInvalidTransition
	)

	switch {
	case errors.Is(err, ErrNotFound):
		httpx.Error(w, r, http.StatusNotFound, httpx.CodeNotFound, ErrNotFound.Error())
	case errors.Is(err, ErrForbidden):
		httpx.Error(w, r, http.StatusForbidden, httpx.CodeForbidden, ErrForbidden.Error())
	case errors.Is(err, ErrIdempotencyKeyMissing):
		httpx.Error(w, r, http.StatusBadRequest, httpx.CodeValidation, ErrIdempotencyKeyMissing.Error())
	case errors.Is(err, ErrIdempotencyKeyReused):
		httpx.Error(w, r, http.StatusConflict, httpx.CodeConflict, ErrIdempotencyKeyReused.Error())
	case errors.As(err, &transitionErr):
		httpx.Error(w, r, http.StatusConflict, httpx.CodeConflict, transitionErr.Error())
	case errors.As(err, &validationErr):
		httpx.ErrorWithFields(w, r, http.StatusBadRequest, httpx.CodeValidation, validationErr.Message,
			map[string]string{validationErr.Field: validationErr.Message})
	default:
		httpx.FromRequest(r).Error(logMessage, "error", err.Error())
		httpx.Error(w, r, http.StatusInternalServerError, httpx.CodeInternal, "внутренняя ошибка сервиса")
	}
}

func toResponse(o Order) orderResponse {
	items := make([]itemResponse, 0, len(o.Items))
	for _, it := range o.Items {
		items = append(items, itemResponse{
			ProductID:      it.ProductID,
			ProductName:    it.ProductName,
			Quantity:       it.Quantity,
			UnitPriceCents: it.UnitPriceCents,
		})
	}

	return orderResponse{
		ID:               o.ID,
		UserID:           o.UserID,
		Status:           o.Status,
		TotalAmountCents: o.TotalAmountCents,
		Currency:         o.Currency,
		CancelReason:     o.CancelReason,
		Items:            items,
		CreatedAt:        o.CreatedAt,
		UpdatedAt:        o.UpdatedAt,
	}
}
