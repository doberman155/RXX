package product

import (
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/hshsb/shop/pkg/httpx"
)

// Handler — REST-слой product-service.
type Handler struct {
	svc *Service
}

// NewHandler создаёт обработчики.
func NewHandler(svc *Service) *Handler { return &Handler{svc: svc} }

// Register настраивает маршруты. Чтение каталога доступно всем,
// изменения — только роли ADMIN.
func (h *Handler) Register(mux *http.ServeMux, protected, admin httpx.Middleware) {
	mux.HandleFunc("GET /api/v1/products", h.list)
	mux.HandleFunc("GET /api/v1/products/{id}", h.get)

	mux.Handle("POST /api/v1/products", protected(admin(http.HandlerFunc(h.create))))
	mux.Handle("PUT /api/v1/products/{id}", protected(admin(http.HandlerFunc(h.update))))
	mux.Handle("DELETE /api/v1/products/{id}", protected(admin(http.HandlerFunc(h.delete))))
}

type productRequest struct {
	SKU         string `json:"sku"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Category    string `json:"category"`
	PriceCents  int64  `json:"price_cents"`
	Stock       int    `json:"stock"`
}

type productResponse struct {
	ID          uuid.UUID `json:"id"`
	SKU         string    `json:"sku"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Category    string    `json:"category"`
	PriceCents  int64     `json:"price_cents"`
	Currency    string    `json:"currency"`
	Stock       int       `json:"stock"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type listResponse struct {
	Items  []productResponse `json:"items"`
	Total  int               `json:"total"`
	Limit  int               `json:"limit"`
	Offset int               `json:"offset"`
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	var req productRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.Error(w, r, http.StatusBadRequest, httpx.CodeBadRequest, err.Error())
		return
	}

	created, err := h.svc.Create(r.Context(), CreateInput{
		SKU:         req.SKU,
		Name:        req.Name,
		Description: req.Description,
		Category:    req.Category,
		PriceCents:  req.PriceCents,
		Stock:       req.Stock,
	})
	if err != nil {
		writeError(w, r, err, "не удалось создать товар")
		return
	}

	httpx.JSON(w, r, http.StatusCreated, toResponse(created))
}

func (h *Handler) update(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}

	var req productRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.Error(w, r, http.StatusBadRequest, httpx.CodeBadRequest, err.Error())
		return
	}

	updated, err := h.svc.Update(r.Context(), id, UpdateInput{
		Name:        req.Name,
		Description: req.Description,
		Category:    req.Category,
		PriceCents:  req.PriceCents,
		Stock:       req.Stock,
	})
	if err != nil {
		writeError(w, r, err, "не удалось обновить товар")
		return
	}

	httpx.JSON(w, r, http.StatusOK, toResponse(updated))
}

func (h *Handler) delete(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}

	if err := h.svc.Delete(r.Context(), id); err != nil {
		writeError(w, r, err, "не удалось удалить товар")
		return
	}

	httpx.NoContent(w, r)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}

	p, err := h.svc.Get(r.Context(), id)
	if err != nil {
		writeError(w, r, err, "не удалось получить товар")
		return
	}

	httpx.JSON(w, r, http.StatusOK, toResponse(p))
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

	products, total, err := h.svc.List(r.Context(), ListFilter{
		Category: r.URL.Query().Get("category"),
		Limit:    limit,
		Offset:   offset,
	})
	if err != nil {
		writeError(w, r, err, "не удалось получить каталог")
		return
	}

	items := make([]productResponse, 0, len(products))
	for _, p := range products {
		items = append(items, toResponse(p))
	}

	httpx.JSON(w, r, http.StatusOK, listResponse{Items: items, Total: total, Limit: limit, Offset: offset})
}

func pathUUID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpx.Error(w, r, http.StatusBadRequest, httpx.CodeBadRequest, "идентификатор товара должен быть UUID")
		return uuid.Nil, false
	}
	return id, true
}

func writeError(w http.ResponseWriter, r *http.Request, err error, logMessage string) {
	var validationErr ValidationError
	switch {
	case errors.Is(err, ErrNotFound):
		httpx.Error(w, r, http.StatusNotFound, httpx.CodeNotFound, ErrNotFound.Error())
	case errors.Is(err, ErrSKUAlreadyUsed):
		httpx.Error(w, r, http.StatusConflict, httpx.CodeConflict, ErrSKUAlreadyUsed.Error())
	case errors.As(err, &validationErr):
		httpx.ErrorWithFields(w, r, http.StatusBadRequest, httpx.CodeValidation, validationErr.Message,
			map[string]string{validationErr.Field: validationErr.Message})
	default:
		httpx.FromRequest(r).Error(logMessage, "error", err.Error())
		httpx.Error(w, r, http.StatusInternalServerError, httpx.CodeInternal, "внутренняя ошибка сервиса")
	}
}

func toResponse(p Product) productResponse {
	return productResponse{
		ID:          p.ID,
		SKU:         p.SKU,
		Name:        p.Name,
		Description: p.Description,
		Category:    p.Category,
		PriceCents:  p.PriceCents,
		Currency:    "RUB",
		Stock:       p.Stock,
		CreatedAt:   p.CreatedAt,
		UpdatedAt:   p.UpdatedAt,
	}
}
