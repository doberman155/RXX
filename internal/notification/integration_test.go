//go:build integration

package notification

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

	"github.com/hshsb/shop/internal/testsupport"
	"github.com/hshsb/shop/migrations"
	"github.com/hshsb/shop/pkg/auth"
	"github.com/hshsb/shop/pkg/config"
	"github.com/hshsb/shop/pkg/events"
	"github.com/hshsb/shop/pkg/idempotency"
	"github.com/hshsb/shop/pkg/kafkax"
)

const testGroup = "notification-service.orders"

type fixture struct {
	repo    *Repository
	svc     *Service
	handler http.Handler
	tokens  *auth.Manager
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	pool := testsupport.NewDatabase(t, migrations.DirNotification)
	repo := NewRepository(pool)
	svc := NewService(repo, testGroup, testsupport.Logger())

	tokens, err := auth.NewManager("секрет-интеграционных-тестов", time.Hour, "user-service")
	require.NoError(t, err)

	handler := buildHandler(config.App{ServiceName: ServiceName}, svc, tokens, repo, testsupport.Logger())

	return &fixture{repo: repo, svc: svc, handler: handler, tokens: tokens}
}

func (f *fixture) token(t *testing.T, userID uuid.UUID, role auth.Role) string {
	t.Helper()
	token, _, err := f.tokens.Issue(userID.String(), "user@shop.local", role)
	require.NoError(t, err)
	return token
}

func (f *fixture) get(t *testing.T, path, token string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, path, strings.NewReader(""))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	return rec
}

func statusChangedEvent(orderID, userID, from, to string) events.OrderStatusChanged {
	return events.OrderStatusChanged{
		Envelope:  events.NewEnvelope(events.TypeOrderStatusChanged, orderID),
		UserID:    userID,
		OldStatus: from,
		NewStatus: to,
	}
}

func TestИнтеграцияУведомленияПоЖизненномуЦиклуЗаказа(t *testing.T) {
	f := newFixture(t)
	orderID, userID := uuid.NewString(), uuid.NewString()

	created := events.OrderCreated{
		Envelope: events.NewEnvelope(events.TypeOrderCreated, orderID),
		UserID:   userID,
		Items:    []events.Item{{ProductID: uuid.NewString(), Quantity: 2}},
	}
	require.NoError(t, f.svc.HandleOrderCreated(context.Background(), created))
	require.NoError(t, f.svc.HandleOrderStatusChanged(context.Background(),
		statusChangedEvent(orderID, userID, "NEW", "RESERVED")))
	require.NoError(t, f.svc.HandleOrderStatusChanged(context.Background(),
		statusChangedEvent(orderID, userID, "RESERVED", "COMPLETED")))

	count, err := f.repo.CountByOrder(context.Background(), orderID)
	require.NoError(t, err)
	require.Equal(t, 3, count)

	items, total, err := f.repo.List(context.Background(), ListFilter{Limit: 10})
	require.NoError(t, err)
	require.Equal(t, 3, total)

	kinds := make([]string, 0, len(items))
	for _, n := range items {
		kinds = append(kinds, n.Kind+":"+n.NewStatus)
		require.NotEmpty(t, n.Message)
	}
	require.ElementsMatch(t, []string{
		"ORDER_CREATED:NEW",
		"ORDER_STATUS_CHANGED:RESERVED",
		"ORDER_STATUS_CHANGED:COMPLETED",
	}, kinds)
}

// Требование ТЗ: повторная доставка одного и того же сообщения не создаёт
// второй эффект. Проверяется отдельным тестом.
func TestИнтеграцияПовторнаяДоставкаНеСоздаётВтороеУведомление(t *testing.T) {
	f := newFixture(t)
	orderID, userID := uuid.NewString(), uuid.NewString()

	evt := statusChangedEvent(orderID, userID, "NEW", "RESERVED")

	for range 5 {
		require.NoError(t, f.svc.HandleOrderStatusChanged(context.Background(), evt))
	}

	count, err := f.repo.CountByOrder(context.Background(), orderID)
	require.NoError(t, err)
	require.Equal(t, 1, count, "пять доставок одного события дают одно уведомление")

	processed, err := idempotency.WasProcessed(context.Background(), f.repo.Pool(), testGroup, evt.EventID)
	require.NoError(t, err)
	require.True(t, processed)
}

func TestИнтеграцияРазныеСобытияОдногоЗаказаНеСхлопываются(t *testing.T) {
	f := newFixture(t)
	orderID, userID := uuid.NewString(), uuid.NewString()

	// Два разных события с одинаковым содержимым, но разными event_id.
	first := statusChangedEvent(orderID, userID, "NEW", "RESERVED")
	second := statusChangedEvent(orderID, userID, "RESERVED", "COMPLETED")

	require.NoError(t, f.svc.HandleOrderStatusChanged(context.Background(), first))
	require.NoError(t, f.svc.HandleOrderStatusChanged(context.Background(), second))

	count, err := f.repo.CountByOrder(context.Background(), orderID)
	require.NoError(t, err)
	require.Equal(t, 2, count)
}

func TestИнтеграцияСписокУведомлений(t *testing.T) {
	f := newFixture(t)
	orderID := uuid.NewString()
	owner := uuid.New()
	stranger := uuid.New()

	require.NoError(t, f.svc.HandleOrderStatusChanged(context.Background(),
		statusChangedEvent(orderID, owner.String(), "NEW", "RESERVED")))
	require.NoError(t, f.svc.HandleOrderStatusChanged(context.Background(),
		statusChangedEvent(uuid.NewString(), stranger.String(), "NEW", "CANCELLED")))

	t.Run("без токена 401", func(t *testing.T) {
		require.Equal(t, http.StatusUnauthorized, f.get(t, "/api/v1/notifications", "").Code)
	})

	t.Run("пользователь видит только свои уведомления", func(t *testing.T) {
		rec := f.get(t, "/api/v1/notifications", f.token(t, owner, auth.RoleUser))
		require.Equal(t, http.StatusOK, rec.Code)

		var list listResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
		require.Equal(t, 1, list.Total)
		require.Equal(t, owner, list.Items[0].UserID)
	})

	t.Run("администратор видит все и может фильтровать", func(t *testing.T) {
		rec := f.get(t, "/api/v1/notifications", f.token(t, stranger, auth.RoleAdmin))
		var list listResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
		require.Equal(t, 2, list.Total)

		rec = f.get(t, "/api/v1/notifications?order_id="+orderID, f.token(t, stranger, auth.RoleAdmin))
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
		require.Equal(t, 1, list.Total)
	})

	t.Run("некорректный фильтр отклоняется", func(t *testing.T) {
		rec := f.get(t, "/api/v1/notifications?order_id=не-uuid", f.token(t, owner, auth.RoleUser))
		require.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestИнтеграцияОбработчикСобытий(t *testing.T) {
	f := newFixture(t)
	handler := EventHandler(f.svc)

	orderID, userID := uuid.NewString(), uuid.NewString()
	payload, err := events.Encode(statusChangedEvent(orderID, userID, "NEW", "RESERVED"))
	require.NoError(t, err)

	require.NoError(t, handler(context.Background(), kafkax.Inbound{
		Topic: events.TopicOrderStatusChanged,
		Key:   orderID,
		Value: payload,
	}))

	count, err := f.repo.CountByOrder(context.Background(), orderID)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	require.ErrorIs(t, handler(context.Background(), kafkax.Inbound{
		Topic: events.TopicOrderStatusChanged,
		Value: []byte(`{`),
	}), kafkax.ErrNonRetryable)

	require.ErrorIs(t, handler(context.Background(), kafkax.Inbound{
		Topic: "чужой.топик",
		Value: []byte(`{}`),
	}), kafkax.ErrNonRetryable)
}
