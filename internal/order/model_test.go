package order

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/hshsb/shop/pkg/auth"
)

func TestПереходыСтатусовРазрешённые(t *testing.T) {
	t.Parallel()

	allowed := []struct {
		from Status
		to   Status
	}{
		{StatusNew, StatusReserved},
		{StatusNew, StatusCancelled},
		{StatusReserved, StatusCompleted},
		{StatusReserved, StatusCancelled},
	}

	for _, tt := range allowed {
		require.Truef(t, tt.from.CanTransitionTo(tt.to), "%s -> %s должен быть разрешён", tt.from, tt.to)
	}
}

func TestПереходыСтатусовЗапрещённые(t *testing.T) {
	t.Parallel()

	forbidden := []struct {
		from Status
		to   Status
	}{
		{StatusNew, StatusCompleted},        // нельзя завершить без резерва
		{StatusNew, StatusNew},              // переход в себя же
		{StatusReserved, StatusNew},         // назад нельзя
		{StatusReserved, StatusReserved},    // повторный резерв
		{StatusCompleted, StatusCancelled},  // терминальный статус
		{StatusCompleted, StatusCompleted},  // терминальный статус
		{StatusCancelled, StatusReserved},   // терминальный статус
		{StatusCancelled, StatusCompleted},  // терминальный статус
		{Status("UNKNOWN"), StatusReserved}, // неизвестный статус
	}

	for _, tt := range forbidden {
		require.Falsef(t, tt.from.CanTransitionTo(tt.to), "%s -> %s должен быть запрещён", tt.from, tt.to)
	}
}

func TestStatusValidИTerminal(t *testing.T) {
	t.Parallel()

	require.True(t, StatusNew.Valid())
	require.True(t, StatusCancelled.Valid())
	require.False(t, Status("PAID").Valid())

	require.False(t, StatusNew.Terminal())
	require.False(t, StatusReserved.Terminal())
	require.True(t, StatusCompleted.Terminal())
	require.True(t, StatusCancelled.Terminal())
}

func TestErrInvalidTransitionСообщаетПереход(t *testing.T) {
	t.Parallel()

	err := ErrInvalidTransition{From: StatusCompleted, To: StatusCancelled}
	require.Contains(t, err.Error(), "COMPLETED -> CANCELLED")
}

func TestCreateInputValidateУспешныйСлучай(t *testing.T) {
	t.Parallel()

	productID := uuid.NewString()
	in := CreateInput{
		Actor:          Actor{UserID: uuid.New(), Role: auth.RoleUser},
		Items:          []CreateItem{{ProductID: productID, Quantity: 2}},
		IdempotencyKey: "key-1",
	}

	items, err := in.Validate()
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, 2, items[0].Quantity)
}

func TestCreateInputValidateСкладываетДубликатыПозиций(t *testing.T) {
	t.Parallel()

	productID := uuid.NewString()
	in := CreateInput{
		Items: []CreateItem{
			{ProductID: productID, Quantity: 2},
			{ProductID: productID, Quantity: 3},
		},
		IdempotencyKey: "key-1",
	}

	items, err := in.Validate()
	require.NoError(t, err)
	require.Len(t, items, 1, "одинаковые товары должны складываться в одну позицию")
	require.Equal(t, 5, items[0].Quantity)
}

func TestCreateInputValidateОшибки(t *testing.T) {
	t.Parallel()

	productID := uuid.NewString()

	tests := []struct {
		name  string
		input CreateInput
		check func(t *testing.T, err error)
	}{
		{
			name:  "без ключа идемпотентности",
			input: CreateInput{Items: []CreateItem{{ProductID: productID, Quantity: 1}}},
			check: func(t *testing.T, err error) { require.ErrorIs(t, err, ErrIdempotencyKeyMissing) },
		},
		{
			name:  "пустой список позиций",
			input: CreateInput{IdempotencyKey: "key"},
			check: func(t *testing.T, err error) { require.ErrorAs(t, err, &ValidationError{}) },
		},
		{
			name: "product_id не UUID",
			input: CreateInput{
				IdempotencyKey: "key",
				Items:          []CreateItem{{ProductID: "не-uuid", Quantity: 1}},
			},
			check: func(t *testing.T, err error) { require.ErrorAs(t, err, &ValidationError{}) },
		},
		{
			name: "нулевое количество",
			input: CreateInput{
				IdempotencyKey: "key",
				Items:          []CreateItem{{ProductID: productID, Quantity: 0}},
			},
			check: func(t *testing.T, err error) { require.ErrorAs(t, err, &ValidationError{}) },
		},
		{
			name: "отрицательное количество",
			input: CreateInput{
				IdempotencyKey: "key",
				Items:          []CreateItem{{ProductID: productID, Quantity: -5}},
			},
			check: func(t *testing.T, err error) { require.ErrorAs(t, err, &ValidationError{}) },
		},
		{
			name: "количество больше предела",
			input: CreateInput{
				IdempotencyKey: "key",
				Items:          []CreateItem{{ProductID: productID, Quantity: MaxQuantityPerItem + 1}},
			},
			check: func(t *testing.T, err error) { require.ErrorAs(t, err, &ValidationError{}) },
		},
		{
			name: "сумма дубликатов превышает предел",
			input: CreateInput{
				IdempotencyKey: "key",
				Items: []CreateItem{
					{ProductID: productID, Quantity: MaxQuantityPerItem},
					{ProductID: productID, Quantity: 1},
				},
			},
			check: func(t *testing.T, err error) { require.ErrorAs(t, err, &ValidationError{}) },
		},
		{
			name:  "слишком длинный ключ",
			input: CreateInput{IdempotencyKey: string(make([]byte, MaxIdempotencyKeyLn+1))},
			check: func(t *testing.T, err error) { require.Error(t, err) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := tt.input.Validate()
			require.Error(t, err)
			tt.check(t, err)
		})
	}
}

func TestCreateInputValidateОтклоняетСлишкомМногоПозиций(t *testing.T) {
	t.Parallel()

	items := make([]CreateItem, 0, MaxItemsPerOrder+1)
	for range MaxItemsPerOrder + 1 {
		items = append(items, CreateItem{ProductID: uuid.NewString(), Quantity: 1})
	}

	_, err := CreateInput{IdempotencyKey: "key", Items: items}.Validate()
	require.Error(t, err)
}

func TestRequestHashНеЗависитОтПорядкаПозиций(t *testing.T) {
	t.Parallel()

	a, b := uuid.NewString(), uuid.NewString()

	first := RequestHash([]CreateItem{{ProductID: a, Quantity: 1}, {ProductID: b, Quantity: 2}})
	second := RequestHash([]CreateItem{{ProductID: b, Quantity: 2}, {ProductID: a, Quantity: 1}})

	require.Equal(t, first, second)
}

func TestRequestHashМеняетсяПриДругомСоставе(t *testing.T) {
	t.Parallel()

	productID := uuid.NewString()

	first := RequestHash([]CreateItem{{ProductID: productID, Quantity: 1}})
	second := RequestHash([]CreateItem{{ProductID: productID, Quantity: 2}})

	require.NotEqual(t, first, second)
}

func TestActorCanAccess(t *testing.T) {
	t.Parallel()

	owner := uuid.New()
	stranger := uuid.New()
	order := Order{UserID: owner}

	require.True(t, Actor{UserID: owner, Role: auth.RoleUser}.CanAccess(order))
	require.False(t, Actor{UserID: stranger, Role: auth.RoleUser}.CanAccess(order))
	require.True(t, Actor{UserID: stranger, Role: auth.RoleAdmin}.CanAccess(order),
		"администратор видит любые заказы")
}
