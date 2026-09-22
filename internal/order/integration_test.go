//go:build integration

package order

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
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
	"github.com/hshsb/shop/pkg/kafkax"
	"github.com/hshsb/shop/pkg/postgres"
)

const testGroup = "order-service.stock"

type fixture struct {
	pool    *postgres.Pool
	repo    *Repository
	svc     *Service
	handler http.Handler
	tokens  *auth.Manager
}

// newFixture поднимает сервис на отдельной базе.
// autoComplete повторяет поведение переменной ORDER_AUTO_COMPLETE.
func newFixture(t *testing.T, autoComplete bool) *fixture {
	t.Helper()

	pool := testsupport.NewDatabase(t, migrations.DirOrder)
	repo := NewRepository(pool)
	svc := NewService(repo, testGroup, autoComplete, testsupport.Logger())

	tokens, err := auth.NewManager("секрет-интеграционных-тестов", time.Hour, "user-service")
	require.NoError(t, err)

	handler := buildHandler(config.App{ServiceName: ServiceName}, svc, tokens, repo, testsupport.Logger())

	return &fixture{pool: pool, repo: repo, svc: svc, handler: handler, tokens: tokens}
}

func (f *fixture) token(t *testing.T, userID uuid.UUID, role auth.Role) string {
	t.Helper()
	token, _, err := f.tokens.Issue(userID.String(), "user@shop.local", role)
	require.NoError(t, err)
	return token
}

func (f *fixture) do(t *testing.T, method, path, body, token string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	return rec
}

func (f *fixture) outboxTopics(t *testing.T) []string {
	t.Helper()

	rows, err := f.pool.Query(context.Background(), `SELECT topic FROM outbox ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()

	var topics []string
	for rows.Next() {
		var topic string
		require.NoError(t, rows.Scan(&topic))
		topics = append(topics, topic)
	}
	require.NoError(t, rows.Err())
	return topics
}

func (f *fixture) statusChanges(t *testing.T) [][2]string {
	t.Helper()

	rows, err := f.pool.Query(context.Background(),
		`SELECT payload FROM outbox WHERE topic = $1 ORDER BY id`, events.TopicOrderStatusChanged)
	require.NoError(t, err)
	defer rows.Close()

	var changes [][2]string
	for rows.Next() {
		var payload []byte
		require.NoError(t, rows.Scan(&payload))

		evt, err := events.Decode[events.OrderStatusChanged](payload)
		require.NoError(t, err)
		changes = append(changes, [2]string{evt.OldStatus, evt.NewStatus})
	}
	require.NoError(t, rows.Err())
	return changes
}

// createOrder создаёт заказ через REST, как это делает клиент.
func (f *fixture) createOrder(t *testing.T, userID uuid.UUID, productID string, qty int, key string) orderResponse {
	t.Helper()

	body := `{"items":[{"product_id":"` + productID + `","quantity":` + strconv.Itoa(qty) + `}]}`
	rec := f.do(t, http.MethodPost, "/api/v1/orders", body, f.token(t, userID, auth.RoleUser),
		map[string]string{HeaderIdempotencyKey: key})
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var created orderResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	return created
}

func reservedEvent(orderID, productID string, qty int, unitPrice int64) events.StockReserved {
	return events.StockReserved{
		Envelope: events.NewEnvelope(events.TypeStockReserved, orderID),
		Items: []events.ReservedItem{{
			ProductID:      productID,
			Name:           "Тестовый товар",
			Quantity:       qty,
			UnitPriceCents: unitPrice,
		}},
		TotalAmountCents: unitPrice * int64(qty),
		Currency:         events.Currency,
	}
}

func rejectedEvent(orderID string) events.StockRejected {
	return events.StockRejected{
		Envelope: events.NewEnvelope(events.TypeStockRejected, orderID),
		Reason:   events.ReasonInsufficientStock,
		Message:  "недостаточно товара на складе",
	}
}

func TestИнтеграцияСозданиеЗаказаПишетСобытиеВOutbox(t *testing.T) {
	f := newFixture(t, true)
	userID, productID := uuid.New(), uuid.NewString()

	created := f.createOrder(t, userID, productID, 2, uuid.NewString())

	require.Equal(t, StatusNew, created.Status)
	require.Equal(t, userID, created.UserID)
	require.Len(t, created.Items, 1)
	require.Equal(t, []string{events.TopicOrderCreated}, f.outboxTopics(t),
		"после создания заказа в outbox должно быть ровно одно событие")

	var payload []byte
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT payload FROM outbox ORDER BY id LIMIT 1`).Scan(&payload))

	evt, err := events.Decode[events.OrderCreated](payload)
	require.NoError(t, err)
	require.Equal(t, created.ID.String(), evt.OrderID)
	require.Equal(t, productID, evt.Items[0].ProductID)
	require.Equal(t, 2, evt.Items[0].Quantity)
}

func TestИнтеграцияIdempotencyKey(t *testing.T) {
	f := newFixture(t, true)
	userID, productID := uuid.New(), uuid.NewString()
	token := f.token(t, userID, auth.RoleUser)
	key := uuid.NewString()

	body := `{"items":[{"product_id":"` + productID + `","quantity":2}]}`

	first := f.do(t, http.MethodPost, "/api/v1/orders", body, token, map[string]string{HeaderIdempotencyKey: key})
	require.Equal(t, http.StatusCreated, first.Code)

	second := f.do(t, http.MethodPost, "/api/v1/orders", body, token, map[string]string{HeaderIdempotencyKey: key})
	require.Equal(t, http.StatusOK, second.Code, "повтор не должен создавать новый заказ")

	var firstOrder, secondOrder orderResponse
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &firstOrder))
	require.NoError(t, json.Unmarshal(second.Body.Bytes(), &secondOrder))
	require.Equal(t, firstOrder.ID, secondOrder.ID)

	require.Len(t, f.outboxTopics(t), 1, "повторный запрос не должен публиковать второе событие")

	t.Run("тот же ключ с другим телом отклоняется", func(t *testing.T) {
		other := `{"items":[{"product_id":"` + productID + `","quantity":5}]}`
		rec := f.do(t, http.MethodPost, "/api/v1/orders", other, token, map[string]string{HeaderIdempotencyKey: key})
		require.Equal(t, http.StatusConflict, rec.Code)
	})

	t.Run("без ключа запрос отклоняется", func(t *testing.T) {
		rec := f.do(t, http.MethodPost, "/api/v1/orders", body, token, nil)
		require.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("порядок позиций не влияет на ключ", func(t *testing.T) {
		second := uuid.NewString()
		mixed := `{"items":[{"product_id":"` + productID + `","quantity":1},{"product_id":"` + second + `","quantity":1}]}`
		reversed := `{"items":[{"product_id":"` + second + `","quantity":1},{"product_id":"` + productID + `","quantity":1}]}`
		orderKey := uuid.NewString()

		a := f.do(t, http.MethodPost, "/api/v1/orders", mixed, token, map[string]string{HeaderIdempotencyKey: orderKey})
		require.Equal(t, http.StatusCreated, a.Code)

		b := f.do(t, http.MethodPost, "/api/v1/orders", reversed, token, map[string]string{HeaderIdempotencyKey: orderKey})
		require.Equal(t, http.StatusOK, b.Code)
	})
}

func TestИнтеграцияСагаУспешныйПуть(t *testing.T) {
	f := newFixture(t, true)
	userID, productID := uuid.New(), uuid.NewString()

	created := f.createOrder(t, userID, productID, 2, uuid.NewString())

	require.NoError(t, f.svc.HandleStockReserved(context.Background(),
		reservedEvent(created.ID.String(), productID, 2, 599000)))

	final, err := f.repo.GetByID(context.Background(), created.ID)
	require.NoError(t, err)
	require.Equal(t, StatusCompleted, final.Status, "при ORDER_AUTO_COMPLETE=true заказ завершается сам")
	require.Equal(t, int64(2*599000), final.TotalAmountCents)
	require.Equal(t, int64(599000), final.Items[0].UnitPriceCents, "цена приезжает из события")
	require.Equal(t, "Тестовый товар", final.Items[0].ProductName)

	require.Equal(t, [][2]string{{"NEW", "RESERVED"}, {"RESERVED", "COMPLETED"}}, f.statusChanges(t))
}

func TestИнтеграцияСагаБезАвтозавершения(t *testing.T) {
	f := newFixture(t, false)
	userID, productID := uuid.New(), uuid.NewString()

	created := f.createOrder(t, userID, productID, 1, uuid.NewString())

	require.NoError(t, f.svc.HandleStockReserved(context.Background(),
		reservedEvent(created.ID.String(), productID, 1, 100)))

	order, err := f.repo.GetByID(context.Background(), created.ID)
	require.NoError(t, err)
	require.Equal(t, StatusReserved, order.Status)
	require.Equal(t, [][2]string{{"NEW", "RESERVED"}}, f.statusChanges(t))

	t.Run("ручное завершение переводит в COMPLETED", func(t *testing.T) {
		rec := f.do(t, http.MethodPost, "/api/v1/orders/"+created.ID.String()+"/complete", "",
			f.token(t, userID, auth.RoleUser), nil)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var completed orderResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &completed))
		require.Equal(t, StatusCompleted, completed.Status)
	})
}

func TestИнтеграцияПовторнаяДоставкаСобытияНеДаётВторогоЭффекта(t *testing.T) {
	f := newFixture(t, true)
	userID, productID := uuid.New(), uuid.NewString()

	created := f.createOrder(t, userID, productID, 2, uuid.NewString())
	evt := reservedEvent(created.ID.String(), productID, 2, 100)

	// Одно и то же событие доставлено трижды.
	for range 3 {
		require.NoError(t, f.svc.HandleStockReserved(context.Background(), evt))
	}

	final, err := f.repo.GetByID(context.Background(), created.ID)
	require.NoError(t, err)
	require.Equal(t, StatusCompleted, final.Status)
	require.Equal(t, int64(200), final.TotalAmountCents)

	require.Equal(t, [][2]string{{"NEW", "RESERVED"}, {"RESERVED", "COMPLETED"}}, f.statusChanges(t),
		"повторная доставка не должна публиковать новые события смены статуса")
}

func TestИнтеграцияОтказВРезервеОтменяетЗаказ(t *testing.T) {
	f := newFixture(t, true)
	userID, productID := uuid.New(), uuid.NewString()

	created := f.createOrder(t, userID, productID, 2, uuid.NewString())
	evt := rejectedEvent(created.ID.String())

	for range 2 {
		require.NoError(t, f.svc.HandleStockRejected(context.Background(), evt))
	}

	final, err := f.repo.GetByID(context.Background(), created.ID)
	require.NoError(t, err)
	require.Equal(t, StatusCancelled, final.Status)
	require.Equal(t, "недостаточно товара на складе", final.CancelReason)
	require.Equal(t, [][2]string{{"NEW", "CANCELLED"}}, f.statusChanges(t))
}

func TestИнтеграцияОтменаПубликуетКомпенсирующееСобытие(t *testing.T) {
	f := newFixture(t, false)
	userID, productID := uuid.New(), uuid.NewString()

	created := f.createOrder(t, userID, productID, 2, uuid.NewString())
	require.NoError(t, f.svc.HandleStockReserved(context.Background(),
		reservedEvent(created.ID.String(), productID, 2, 100)))

	rec := f.do(t, http.MethodPost, "/api/v1/orders/"+created.ID.String()+"/cancel",
		`{"reason":"передумал"}`, f.token(t, userID, auth.RoleUser), nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	topics := f.outboxTopics(t)
	require.Contains(t, topics, events.TopicOrderCancelled,
		"после отмены резерва должно уйти компенсирующее событие")

	var payload []byte
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT payload FROM outbox WHERE topic = $1`, events.TopicOrderCancelled).Scan(&payload))

	evt, err := events.Decode[events.OrderCancelled](payload)
	require.NoError(t, err)
	require.True(t, evt.StockWasReserved)
	require.Equal(t, StatusReserved.String(), evt.PreviousStatus)
	require.Equal(t, "передумал", evt.Reason)
}

func TestИнтеграцияРезервПоОтменённомуЗаказуИгнорируется(t *testing.T) {
	f := newFixture(t, true)
	userID, productID := uuid.New(), uuid.NewString()

	created := f.createOrder(t, userID, productID, 1, uuid.NewString())

	rec := f.do(t, http.MethodPost, "/api/v1/orders/"+created.ID.String()+"/cancel", "",
		f.token(t, userID, auth.RoleUser), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Резерв успел выполниться до отмены: заказ должен остаться CANCELLED,
	// а остаток вернёт компенсирующее событие, опубликованное при отмене.
	require.NoError(t, f.svc.HandleStockReserved(context.Background(),
		reservedEvent(created.ID.String(), productID, 1, 100)))

	final, err := f.repo.GetByID(context.Background(), created.ID)
	require.NoError(t, err)
	require.Equal(t, StatusCancelled, final.Status)
}

func TestИнтеграцияНедопустимыеПереходыЧерезREST(t *testing.T) {
	f := newFixture(t, true)
	userID, productID := uuid.New(), uuid.NewString()
	token := f.token(t, userID, auth.RoleUser)

	created := f.createOrder(t, userID, productID, 1, uuid.NewString())

	t.Run("завершить заказ в статусе NEW нельзя", func(t *testing.T) {
		rec := f.do(t, http.MethodPost, "/api/v1/orders/"+created.ID.String()+"/complete", "", token, nil)
		require.Equal(t, http.StatusConflict, rec.Code)
	})

	require.NoError(t, f.svc.HandleStockReserved(context.Background(),
		reservedEvent(created.ID.String(), productID, 1, 100)))

	t.Run("отменить завершённый заказ нельзя", func(t *testing.T) {
		rec := f.do(t, http.MethodPost, "/api/v1/orders/"+created.ID.String()+"/cancel", "", token, nil)
		require.Equal(t, http.StatusConflict, rec.Code)
	})
}

func TestИнтеграцияДоступКЗаказам(t *testing.T) {
	f := newFixture(t, true)
	owner, stranger := uuid.New(), uuid.New()

	created := f.createOrder(t, owner, uuid.NewString(), 1, uuid.NewString())

	t.Run("без токена 401", func(t *testing.T) {
		require.Equal(t, http.StatusUnauthorized,
			f.do(t, http.MethodGet, "/api/v1/orders/"+created.ID.String(), "", "", nil).Code)
	})

	t.Run("чужой заказ не виден", func(t *testing.T) {
		rec := f.do(t, http.MethodGet, "/api/v1/orders/"+created.ID.String(), "",
			f.token(t, stranger, auth.RoleUser), nil)
		require.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("администратор видит любой заказ", func(t *testing.T) {
		rec := f.do(t, http.MethodGet, "/api/v1/orders/"+created.ID.String(), "",
			f.token(t, stranger, auth.RoleAdmin), nil)
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("список пользователя содержит только его заказы", func(t *testing.T) {
		f.createOrder(t, stranger, uuid.NewString(), 1, uuid.NewString())

		rec := f.do(t, http.MethodGet, "/api/v1/orders", "", f.token(t, owner, auth.RoleUser), nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var list listResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
		require.Equal(t, 1, list.Total)
		require.Equal(t, owner, list.Items[0].UserID)
	})

	t.Run("администратор видит все заказы", func(t *testing.T) {
		rec := f.do(t, http.MethodGet, "/api/v1/orders", "", f.token(t, stranger, auth.RoleAdmin), nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var list listResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
		require.Equal(t, 2, list.Total)
	})

	t.Run("фильтр по неизвестному статусу отклоняется", func(t *testing.T) {
		rec := f.do(t, http.MethodGet, "/api/v1/orders?status=PAID", "", f.token(t, owner, auth.RoleUser), nil)
		require.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestИнтеграцияОбработчикСобытийОтклоняетБитоеСообщение(t *testing.T) {
	f := newFixture(t, true)
	handler := EventHandler(f.svc)

	err := handler(context.Background(), kafkax.Inbound{
		Topic: events.TopicStockReserved,
		Value: []byte(`{"broken":`),
	})
	require.ErrorIs(t, err, kafkax.ErrNonRetryable)

	err = handler(context.Background(), kafkax.Inbound{Topic: "чужой.топик", Value: []byte(`{}`)})
	require.ErrorIs(t, err, kafkax.ErrNonRetryable)
}

func TestИнтеграцияСобытиеПоНесуществующемуЗаказу(t *testing.T) {
	f := newFixture(t, true)

	err := f.svc.HandleStockReserved(context.Background(),
		reservedEvent(uuid.NewString(), uuid.NewString(), 1, 100))
	require.ErrorIs(t, err, ErrNotFound, "ошибка должна привести к повтору и затем к DLQ")
}
