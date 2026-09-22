package events_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/hshsb/shop/pkg/events"
)

func TestTopicsСодержитВсеПятьТопиков(t *testing.T) {
	t.Parallel()

	require.ElementsMatch(t, []string{
		"order.created",
		"stock.reserved",
		"stock.rejected",
		"order.cancelled",
		"order.status-changed",
	}, events.Topics())
}

func TestNewEnvelopeЗаполняетОбязательныеПоля(t *testing.T) {
	t.Parallel()

	orderID := uuid.NewString()
	env := events.NewEnvelope(events.TypeOrderCreated, orderID)

	require.NoError(t, uuid.Validate(env.EventID))
	require.Equal(t, events.TypeOrderCreated, env.EventType)
	require.Equal(t, orderID, env.OrderID)
	require.WithinDuration(t, time.Now(), env.OccurredAt, 5*time.Second)
	require.NoError(t, env.Validate(events.TypeOrderCreated))
}

func TestEnvelopeValidate(t *testing.T) {
	t.Parallel()

	valid := events.NewEnvelope(events.TypeOrderCreated, uuid.NewString())

	tests := []struct {
		name    string
		mutate  func(e *events.Envelope)
		expects string
	}{
		{"пустой event_id", func(e *events.Envelope) { e.EventID = "" }, "event_id"},
		{"event_id не UUID", func(e *events.Envelope) { e.EventID = "не-uuid" }, "UUID"},
		{"пустой order_id", func(e *events.Envelope) { e.OrderID = "" }, "order_id"},
		{"нулевое время", func(e *events.Envelope) { e.OccurredAt = time.Time{} }, "occurred_at"},
		{"чужой тип", func(e *events.Envelope) { e.EventType = events.TypeStockReserved }, "ожидался тип"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			env := valid
			tt.mutate(&env)

			err := env.Validate(events.TypeOrderCreated)
			require.ErrorIs(t, err, events.ErrInvalidEvent)
			require.Contains(t, err.Error(), tt.expects)
		})
	}
}

func TestOrderCreatedValidate(t *testing.T) {
	t.Parallel()

	base := func() events.OrderCreated {
		return events.OrderCreated{
			Envelope: events.NewEnvelope(events.TypeOrderCreated, uuid.NewString()),
			UserID:   uuid.NewString(),
			Items:    []events.Item{{ProductID: uuid.NewString(), Quantity: 2}},
		}
	}

	require.NoError(t, base().Validate())

	noUser := base()
	noUser.UserID = ""
	require.ErrorIs(t, noUser.Validate(), events.ErrInvalidEvent)

	noItems := base()
	noItems.Items = nil
	require.ErrorIs(t, noItems.Validate(), events.ErrInvalidEvent)

	zeroQty := base()
	zeroQty.Items[0].Quantity = 0
	require.ErrorIs(t, zeroQty.Validate(), events.ErrInvalidEvent)

	noProduct := base()
	noProduct.Items[0].ProductID = ""
	require.ErrorIs(t, noProduct.Validate(), events.ErrInvalidEvent)
}

func TestStockReservedValidate(t *testing.T) {
	t.Parallel()

	evt := events.StockReserved{
		Envelope:         events.NewEnvelope(events.TypeStockReserved, uuid.NewString()),
		Items:            []events.ReservedItem{{ProductID: uuid.NewString(), Quantity: 1, UnitPriceCents: 100}},
		TotalAmountCents: 100,
		Currency:         events.Currency,
	}
	require.NoError(t, evt.Validate())

	empty := evt
	empty.Items = nil
	require.ErrorIs(t, empty.Validate(), events.ErrInvalidEvent)

	negative := evt
	negative.TotalAmountCents = -1
	require.ErrorIs(t, negative.Validate(), events.ErrInvalidEvent)
}

func TestStockRejectedValidate(t *testing.T) {
	t.Parallel()

	evt := events.StockRejected{
		Envelope: events.NewEnvelope(events.TypeStockRejected, uuid.NewString()),
		Reason:   events.ReasonInsufficientStock,
		Message:  "мало товара",
	}
	require.NoError(t, evt.Validate())

	noReason := evt
	noReason.Reason = ""
	require.ErrorIs(t, noReason.Validate(), events.ErrInvalidEvent)
}

func TestOrderStatusChangedValidate(t *testing.T) {
	t.Parallel()

	evt := events.OrderStatusChanged{
		Envelope:  events.NewEnvelope(events.TypeOrderStatusChanged, uuid.NewString()),
		UserID:    uuid.NewString(),
		OldStatus: "NEW",
		NewStatus: "RESERVED",
	}
	require.NoError(t, evt.Validate())

	noStatus := evt
	noStatus.NewStatus = ""
	require.ErrorIs(t, noStatus.Validate(), events.ErrInvalidEvent)
}

func TestOrderCancelledValidate(t *testing.T) {
	t.Parallel()

	evt := events.OrderCancelled{
		Envelope:       events.NewEnvelope(events.TypeOrderCancelled, uuid.NewString()),
		UserID:         uuid.NewString(),
		PreviousStatus: "RESERVED",
		Reason:         "передумал",
	}
	require.NoError(t, evt.Validate())
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	original := events.OrderCreated{
		Envelope: events.NewEnvelope(events.TypeOrderCreated, uuid.NewString()),
		UserID:   uuid.NewString(),
		Items:    []events.Item{{ProductID: uuid.NewString(), Quantity: 3}},
	}

	payload, err := events.Encode(original)
	require.NoError(t, err)

	decoded, err := events.Decode[events.OrderCreated](payload)
	require.NoError(t, err)
	require.Equal(t, original.EventID, decoded.EventID)
	require.Equal(t, original.Items, decoded.Items)
	require.Equal(t, original.UserID, decoded.UserID)
}

func TestDecodeОтклоняетБитыйJSON(t *testing.T) {
	t.Parallel()

	_, err := events.Decode[events.OrderCreated]([]byte(`{"broken":`))
	require.ErrorIs(t, err, events.ErrInvalidEvent)
}

func TestDecodeПроверяетКонтракт(t *testing.T) {
	t.Parallel()

	// JSON корректен, но событие не проходит валидацию: нет позиций.
	payload, err := json.Marshal(events.OrderCreated{
		Envelope: events.NewEnvelope(events.TypeOrderCreated, uuid.NewString()),
		UserID:   uuid.NewString(),
	})
	require.NoError(t, err)

	_, err = events.Decode[events.OrderCreated](payload)
	require.ErrorIs(t, err, events.ErrInvalidEvent)
}

func TestPeekEnvelope(t *testing.T) {
	t.Parallel()

	orderID := uuid.NewString()
	payload, err := events.Encode(events.StockReserved{
		Envelope: events.NewEnvelope(events.TypeStockReserved, orderID),
	})
	require.NoError(t, err)

	env, err := events.PeekEnvelope(payload)
	require.NoError(t, err)
	require.Equal(t, orderID, env.OrderID)
	require.Equal(t, events.TypeStockReserved, env.EventType)

	_, err = events.PeekEnvelope([]byte("не json"))
	require.ErrorIs(t, err, events.ErrInvalidEvent)
}

func TestСобытиеСериализуетсяВОжидаемыеПоля(t *testing.T) {
	t.Parallel()

	payload, err := events.Encode(events.OrderCreated{
		Envelope: events.NewEnvelope(events.TypeOrderCreated, uuid.NewString()),
		UserID:   uuid.NewString(),
		Items:    []events.Item{{ProductID: uuid.NewString(), Quantity: 1}},
	})
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(payload, &raw))

	// Поля контракта из ТЗ должны присутствовать у каждого события.
	for _, key := range []string{"event_id", "event_type", "occurred_at", "order_id"} {
		require.Contains(t, raw, key)
	}
}
