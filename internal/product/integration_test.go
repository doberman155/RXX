//go:build integration

package product

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
	"github.com/hshsb/shop/pkg/kafkax"
	"github.com/hshsb/shop/pkg/postgres"
)

// inbound собирает сообщение Kafka для проверки обработчика.
func inbound(topic string, value []byte) kafkax.Inbound {
	return kafkax.Inbound{Topic: topic, Key: uuid.NewString(), Value: value, Timestamp: time.Now()}
}

const testGroup = "product-service.orders"

type fixture struct {
	pool    *postgres.Pool
	repo    *Repository
	svc     *Service
	handler http.Handler
	tokens  *auth.Manager
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	pool := testsupport.NewDatabase(t, migrations.DirProduct)
	repo := NewRepository(pool)
	svc := NewService(repo, testGroup, testsupport.Logger())

	tokens, err := auth.NewManager("секрет-интеграционных-тестов", time.Hour, "user-service")
	require.NoError(t, err)

	cfg := config.App{ServiceName: ServiceName}
	handler := buildHandler(cfg, svc, tokens, repo, testsupport.Logger())

	return &fixture{pool: pool, repo: repo, svc: svc, handler: handler, tokens: tokens}
}

func (f *fixture) token(t *testing.T, role auth.Role) string {
	t.Helper()
	token, _, err := f.tokens.Issue(uuid.NewString(), "user@shop.local", role)
	require.NoError(t, err)
	return token
}

func (f *fixture) do(t *testing.T, method, path, body, token string) *httptest.ResponseRecorder {
	t.Helper()

	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}

	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	return rec
}

func (f *fixture) newProduct(t *testing.T, stock int, priceCents int64) Product {
	t.Helper()

	p, err := f.svc.Create(context.Background(), CreateInput{
		SKU:        "SKU-" + uuid.NewString()[:8],
		Name:       "Тестовый товар",
		Category:   "test",
		PriceCents: priceCents,
		Stock:      stock,
	})
	require.NoError(t, err)
	return p
}

func (f *fixture) stock(t *testing.T, id uuid.UUID) int {
	t.Helper()

	p, err := f.repo.GetByID(context.Background(), id)
	require.NoError(t, err)
	return p.Stock
}

func (f *fixture) outboxCount(t *testing.T, topic string) int {
	t.Helper()

	var count int
	err := f.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE topic = $1`, topic).Scan(&count)
	require.NoError(t, err)
	return count
}

func orderCreatedEvent(orderID string, items ...events.Item) events.OrderCreated {
	return events.OrderCreated{
		Envelope: events.NewEnvelope(events.TypeOrderCreated, orderID),
		UserID:   uuid.NewString(),
		Items:    items,
	}
}

func TestИнтеграцияCRUDКаталога(t *testing.T) {
	f := newFixture(t)
	adminToken := f.token(t, auth.RoleAdmin)
	userToken := f.token(t, auth.RoleUser)

	body := `{"sku":"SKU-INT-1","name":"Кофемолка","description":"Ручная","category":"kitchen","price_cents":599000,"stock":7}`

	t.Run("создание доступно только ADMIN", func(t *testing.T) {
		require.Equal(t, http.StatusUnauthorized, f.do(t, http.MethodPost, "/api/v1/products", body, "").Code)
		require.Equal(t, http.StatusForbidden, f.do(t, http.MethodPost, "/api/v1/products", body, userToken).Code)
	})

	var created productResponse
	t.Run("ADMIN создаёт товар", func(t *testing.T) {
		rec := f.do(t, http.MethodPost, "/api/v1/products", body, adminToken)
		require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
		require.Equal(t, 7, created.Stock)
	})

	t.Run("повторный SKU отклоняется", func(t *testing.T) {
		require.Equal(t, http.StatusConflict, f.do(t, http.MethodPost, "/api/v1/products", body, adminToken).Code)
	})

	t.Run("чтение доступно без токена", func(t *testing.T) {
		rec := f.do(t, http.MethodGet, "/api/v1/products/"+created.ID.String(), "", "")
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("фильтр по категории и пагинация", func(t *testing.T) {
		rec := f.do(t, http.MethodGet, "/api/v1/products?category=kitchen&limit=10&offset=0", "", "")
		require.Equal(t, http.StatusOK, rec.Code)

		var list listResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
		require.Equal(t, 1, list.Total)

		rec = f.do(t, http.MethodGet, "/api/v1/products?category=другое", "", "")
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
		require.Equal(t, 0, list.Total)
	})

	t.Run("обновление меняет карточку", func(t *testing.T) {
		update := `{"sku":"","name":"Кофемолка PRO","description":"","category":"kitchen","price_cents":700000,"stock":3}`
		rec := f.do(t, http.MethodPut, "/api/v1/products/"+created.ID.String(), update, adminToken)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var updated productResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
		require.Equal(t, "Кофемолка PRO", updated.Name)
		require.Equal(t, 3, updated.Stock)
	})

	t.Run("удаление скрывает товар", func(t *testing.T) {
		require.Equal(t, http.StatusNoContent,
			f.do(t, http.MethodDelete, "/api/v1/products/"+created.ID.String(), "", adminToken).Code)
		require.Equal(t, http.StatusNotFound,
			f.do(t, http.MethodGet, "/api/v1/products/"+created.ID.String(), "", "").Code)
		require.Equal(t, http.StatusNotFound,
			f.do(t, http.MethodDelete, "/api/v1/products/"+created.ID.String(), "", adminToken).Code)
	})

	t.Run("некорректный id отклоняется", func(t *testing.T) {
		require.Equal(t, http.StatusBadRequest, f.do(t, http.MethodGet, "/api/v1/products/не-uuid", "", "").Code)
	})

	t.Run("валидация тела", func(t *testing.T) {
		rec := f.do(t, http.MethodPost, "/api/v1/products", `{"sku":"","name":"x","category":"c","price_cents":1,"stock":1}`, adminToken)
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "sku")
	})
}

func TestИнтеграцияРезервСписываетОстатокИПишетСобытие(t *testing.T) {
	f := newFixture(t)
	product := f.newProduct(t, 10, 599000)

	evt := orderCreatedEvent(uuid.NewString(), events.Item{ProductID: product.ID.String(), Quantity: 3})
	require.NoError(t, f.svc.HandleOrderCreated(context.Background(), evt))

	require.Equal(t, 7, f.stock(t, product.ID), "остаток должен уменьшиться на 3")
	require.Equal(t, 1, f.outboxCount(t, events.TopicStockReserved))
	require.Equal(t, 0, f.outboxCount(t, events.TopicStockRejected))

	payload := f.outboxPayload(t, events.TopicStockReserved)
	reserved, err := events.Decode[events.StockReserved](payload)
	require.NoError(t, err)
	require.Equal(t, evt.OrderID, reserved.OrderID)
	require.Equal(t, int64(3*599000), reserved.TotalAmountCents)
	require.Equal(t, "Тестовый товар", reserved.Items[0].Name)
}

func (f *fixture) outboxPayload(t *testing.T, topic string) []byte {
	t.Helper()

	var payload []byte
	err := f.pool.QueryRow(context.Background(),
		`SELECT payload FROM outbox WHERE topic = $1 ORDER BY id LIMIT 1`, topic).Scan(&payload)
	require.NoError(t, err)
	return payload
}

func TestИнтеграцияПовторнаяДоставкаНеДаётВторогоЭффекта(t *testing.T) {
	f := newFixture(t)
	product := f.newProduct(t, 10, 100)

	evt := orderCreatedEvent(uuid.NewString(), events.Item{ProductID: product.ID.String(), Quantity: 4})

	// Одно и то же сообщение доставлено трижды (at-least-once у Kafka).
	for range 3 {
		require.NoError(t, f.svc.HandleOrderCreated(context.Background(), evt))
	}

	require.Equal(t, 6, f.stock(t, product.ID), "остаток должен быть списан ровно один раз")
	require.Equal(t, 1, f.outboxCount(t, events.TopicStockReserved), "событие должно быть опубликовано один раз")

	var reservations int
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM reservations WHERE order_id = $1`, evt.OrderID).Scan(&reservations))
	require.Equal(t, 1, reservations)
}

func TestИнтеграцияОтказВРезервеПриНехваткеОстатка(t *testing.T) {
	f := newFixture(t)
	product := f.newProduct(t, 2, 100)

	evt := orderCreatedEvent(uuid.NewString(), events.Item{ProductID: product.ID.String(), Quantity: 5})
	require.NoError(t, f.svc.HandleOrderCreated(context.Background(), evt))

	require.Equal(t, 2, f.stock(t, product.ID), "остаток не должен меняться при отказе")
	require.Equal(t, 1, f.outboxCount(t, events.TopicStockRejected))
	require.Equal(t, 0, f.outboxCount(t, events.TopicStockReserved))

	rejected, err := events.Decode[events.StockRejected](f.outboxPayload(t, events.TopicStockRejected))
	require.NoError(t, err)
	require.Equal(t, events.ReasonInsufficientStock, rejected.Reason)
	require.Len(t, rejected.Items, 1)
	require.Equal(t, 5, rejected.Items[0].Requested)
	require.Equal(t, 2, rejected.Items[0].Available)
}

func TestИнтеграцияОтказЕслиТоварНеНайден(t *testing.T) {
	f := newFixture(t)

	evt := orderCreatedEvent(uuid.NewString(), events.Item{ProductID: uuid.NewString(), Quantity: 1})
	require.NoError(t, f.svc.HandleOrderCreated(context.Background(), evt))

	rejected, err := events.Decode[events.StockRejected](f.outboxPayload(t, events.TopicStockRejected))
	require.NoError(t, err)
	require.Equal(t, events.ReasonProductNotFound, rejected.Reason)
}

func TestИнтеграцияРезервВсёИлиНичего(t *testing.T) {
	f := newFixture(t)
	enough := f.newProduct(t, 10, 100)
	scarce := f.newProduct(t, 1, 100)

	evt := orderCreatedEvent(uuid.NewString(),
		events.Item{ProductID: enough.ID.String(), Quantity: 2},
		events.Item{ProductID: scarce.ID.String(), Quantity: 5},
	)
	require.NoError(t, f.svc.HandleOrderCreated(context.Background(), evt))

	require.Equal(t, 10, f.stock(t, enough.ID), "доступный товар не должен списываться при общем отказе")
	require.Equal(t, 1, f.stock(t, scarce.ID))
	require.Equal(t, 1, f.outboxCount(t, events.TopicStockRejected))
}

func TestИнтеграцияКомпенсацияВозвращаетОстаток(t *testing.T) {
	f := newFixture(t)
	product := f.newProduct(t, 10, 100)
	orderID := uuid.NewString()

	require.NoError(t, f.svc.HandleOrderCreated(context.Background(),
		orderCreatedEvent(orderID, events.Item{ProductID: product.ID.String(), Quantity: 4})))
	require.Equal(t, 6, f.stock(t, product.ID))

	cancelled := events.OrderCancelled{
		Envelope:         events.NewEnvelope(events.TypeOrderCancelled, orderID),
		UserID:           uuid.NewString(),
		PreviousStatus:   "RESERVED",
		Reason:           "передумал",
		StockWasReserved: true,
	}

	// Компенсация тоже идемпотентна: повтор не должен вернуть остаток дважды.
	for range 3 {
		require.NoError(t, f.svc.HandleOrderCancelled(context.Background(), cancelled))
	}

	require.Equal(t, 10, f.stock(t, product.ID), "остаток должен вернуться ровно один раз")

	var released int
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM reservations WHERE order_id = $1 AND status = 'RELEASED'`, orderID).Scan(&released))
	require.Equal(t, 1, released)
}

func TestИнтеграцияОтменаБезБрониБезопасна(t *testing.T) {
	f := newFixture(t)

	cancelled := events.OrderCancelled{
		Envelope:       events.NewEnvelope(events.TypeOrderCancelled, uuid.NewString()),
		UserID:         uuid.NewString(),
		PreviousStatus: "NEW",
	}
	require.NoError(t, f.svc.HandleOrderCancelled(context.Background(), cancelled))
}

func TestИнтеграцияОбработчикСобытийОтправляетБитыйJSONвDLQ(t *testing.T) {
	f := newFixture(t)
	handler := EventHandler(f.svc)

	err := handler(context.Background(), inbound(events.TopicOrderCreated, []byte(`{"broken":`)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "не подлежит повторной обработке")

	err = handler(context.Background(), inbound("неизвестный.топик", []byte(`{}`)))
	require.Error(t, err)
}
