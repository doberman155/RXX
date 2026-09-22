package notification

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCreatedMessageСодержитЗаказИКоличество(t *testing.T) {
	t.Parallel()

	orderID := uuid.NewString()
	msg := createdMessage(orderID, 3)

	require.Contains(t, msg, orderID)
	require.Contains(t, msg, "3")
}

func TestStatusMessageБезПричины(t *testing.T) {
	t.Parallel()

	orderID := uuid.NewString()
	msg := statusMessage(orderID, "NEW", "RESERVED", "")

	require.Contains(t, msg, orderID)
	require.Contains(t, msg, "NEW")
	require.Contains(t, msg, "RESERVED")
	require.NotContains(t, msg, "Причина")
}

func TestStatusMessageСПричиной(t *testing.T) {
	t.Parallel()

	msg := statusMessage(uuid.NewString(), "NEW", "CANCELLED", "недостаточно товара")

	require.Contains(t, msg, "CANCELLED")
	require.Contains(t, msg, "Причина: недостаточно товара")
}

func TestParseIDs(t *testing.T) {
	t.Parallel()

	orderID, userID := uuid.NewString(), uuid.NewString()

	gotOrder, gotUser, err := parseIDs(orderID, userID)
	require.NoError(t, err)
	require.Equal(t, orderID, gotOrder.String())
	require.Equal(t, userID, gotUser.String())

	_, _, err = parseIDs("не-uuid", userID)
	require.Error(t, err)

	_, _, err = parseIDs(orderID, "не-uuid")
	require.Error(t, err)
}
