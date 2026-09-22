package product

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/hshsb/shop/pkg/events"
)

func validInput() CreateInput {
	return CreateInput{
		SKU:         "SKU-001",
		Name:        "Кофемолка",
		Description: "Ручная кофемолка",
		Category:    "kitchen",
		PriceCents:  599000,
		Stock:       10,
	}
}

func TestCreateInputValidateПропускаетКорректныеДанные(t *testing.T) {
	t.Parallel()
	require.NoError(t, validInput().Validate())
}

func TestCreateInputValidateОшибки(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(in *CreateInput)
		field  string
	}{
		{"пустой sku", func(in *CreateInput) { in.SKU = "  " }, "sku"},
		{"длинный sku", func(in *CreateInput) { in.SKU = strings.Repeat("a", 65) }, "sku"},
		{"пустое имя", func(in *CreateInput) { in.Name = "" }, "name"},
		{"длинное имя", func(in *CreateInput) { in.Name = strings.Repeat("я", 256) }, "name"},
		{"пустая категория", func(in *CreateInput) { in.Category = "" }, "category"},
		{"длинное описание", func(in *CreateInput) { in.Description = strings.Repeat("a", 2001) }, "description"},
		{"отрицательная цена", func(in *CreateInput) { in.PriceCents = -1 }, "price_cents"},
		{"отрицательный остаток", func(in *CreateInput) { in.Stock = -1 }, "stock"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := validInput()
			tt.mutate(&in)

			err := in.Validate()
			require.Error(t, err)

			var validationErr ValidationError
			require.ErrorAs(t, err, &validationErr)
			require.Equal(t, tt.field, validationErr.Field)
		})
	}
}

func TestUpdateInputValidate(t *testing.T) {
	t.Parallel()

	in := UpdateInput{Name: "Кофемолка", Category: "kitchen", PriceCents: 100, Stock: 1}
	require.NoError(t, in.Validate())

	in.Name = ""
	require.Error(t, in.Validate())
}

func TestAggregateItemsСкладываетОдинаковыеТовары(t *testing.T) {
	t.Parallel()

	a, b := uuid.NewString(), uuid.NewString()

	got, err := aggregateItems([]events.Item{
		{ProductID: a, Quantity: 1},
		{ProductID: b, Quantity: 4},
		{ProductID: a, Quantity: 2},
	})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, a, got[0].ProductID)
	require.Equal(t, 3, got[0].Quantity, "количества одного товара должны складываться")
	require.Equal(t, b, got[1].ProductID)
	require.Equal(t, 4, got[1].Quantity)
}

func TestAggregateItemsОтклоняетНеUUID(t *testing.T) {
	t.Parallel()

	_, err := aggregateItems([]events.Item{{ProductID: "не-uuid", Quantity: 1}})
	require.Error(t, err)
}

func TestBuildRejectionОпределяетПричину(t *testing.T) {
	t.Parallel()

	known := uuid.New()
	missing := uuid.New()

	locked := map[uuid.UUID]lockedProduct{known: {id: known, stock: 1}}

	outcome := buildRejection(locked, []events.RejectedItem{
		{ProductID: known.String(), Requested: 5, Available: 1},
	})
	require.False(t, outcome.Reserved)
	require.Equal(t, events.ReasonInsufficientStock, outcome.Reason)

	outcome = buildRejection(locked, []events.RejectedItem{
		{ProductID: missing.String(), Requested: 1, Available: 0},
	})
	require.Equal(t, events.ReasonProductNotFound, outcome.Reason)
	require.NotEmpty(t, outcome.Message)
}
