package notification

import (
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/hshsb/shop/pkg/auth"
	"github.com/hshsb/shop/pkg/httpx"
)

// Handler — REST-слой notification-service.
// Сервис получает данные только из Kafka; HTTP нужен для /healthz и для
// просмотра сохранённых уведомлений при демонстрации и в тестах.
type Handler struct {
	svc *Service
}

// NewHandler создаёт обработчики.
func NewHandler(svc *Service) *Handler { return &Handler{svc: svc} }

// Register настраивает маршруты.
func (h *Handler) Register(mux *http.ServeMux, protected httpx.Middleware) {
	mux.Handle("GET /api/v1/notifications", protected(http.HandlerFunc(h.list)))
}

type notificationResponse struct {
	ID        uuid.UUID `json:"id"`
	OrderID   uuid.UUID `json:"order_id"`
	UserID    uuid.UUID `json:"user_id"`
	EventID   uuid.UUID `json:"event_id"`
	EventType string    `json:"event_type"`
	Kind      string    `json:"kind"`
	OldStatus string    `json:"old_status,omitempty"`
	NewStatus string    `json:"new_status"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

type listResponse struct {
	Items  []notificationResponse `json:"items"`
	Total  int                    `json:"total"`
	Limit  int                    `json:"limit"`
	Offset int                    `json:"offset"`
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	claims := httpx.MustClaims(r)

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

	filter := ListFilter{Limit: limit, Offset: offset}

	if raw := r.URL.Query().Get("order_id"); raw != "" {
		orderID, err := uuid.Parse(raw)
		if err != nil {
			httpx.Error(w, r, http.StatusBadRequest, httpx.CodeValidation, "order_id должен быть UUID")
			return
		}
		filter.OrderID = &orderID
	}

	// Обычный пользователь видит только свои уведомления.
	if claims.Role != auth.RoleAdmin {
		userID, err := uuid.Parse(claims.UserID())
		if err != nil {
			httpx.Error(w, r, http.StatusUnauthorized, httpx.CodeUnauthorized, "некорректный идентификатор пользователя в токене")
			return
		}
		filter.UserID = &userID
	} else if raw := r.URL.Query().Get("user_id"); raw != "" {
		userID, err := uuid.Parse(raw)
		if err != nil {
			httpx.Error(w, r, http.StatusBadRequest, httpx.CodeValidation, "user_id должен быть UUID")
			return
		}
		filter.UserID = &userID
	}

	notifications, total, err := h.svc.List(r.Context(), filter)
	if err != nil {
		httpx.FromRequest(r).Error("не удалось получить уведомления", "error", err.Error())
		httpx.Error(w, r, http.StatusInternalServerError, httpx.CodeInternal, "внутренняя ошибка сервиса")
		return
	}

	items := make([]notificationResponse, 0, len(notifications))
	for _, n := range notifications {
		items = append(items, notificationResponse{
			ID:        n.ID,
			OrderID:   n.OrderID,
			UserID:    n.UserID,
			EventID:   n.EventID,
			EventType: n.EventType,
			Kind:      n.Kind,
			OldStatus: n.OldStatus,
			NewStatus: n.NewStatus,
			Message:   n.Message,
			CreatedAt: n.CreatedAt,
		})
	}

	httpx.JSON(w, r, http.StatusOK, listResponse{Items: items, Total: total, Limit: limit, Offset: offset})
}
