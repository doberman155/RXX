package user

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeEmail(t *testing.T) {
	t.Parallel()
	require.Equal(t, "user@shop.local", NormalizeEmail("  User@Shop.Local  "))
}

func TestValidateEmail(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateEmail("user@shop.local"))
	require.NoError(t, ValidateEmail(" USER@shop.local "))

	for _, bad := range []string{"", "   ", "не-почта", "user@", "@shop.local", strings.Repeat("a", 250) + "@shop.local"} {
		err := ValidateEmail(bad)
		require.Errorf(t, err, "адрес %q должен быть отклонён", bad)

		var validationErr ValidationError
		require.ErrorAs(t, err, &validationErr)
		require.Equal(t, "email", validationErr.Field)
	}
}

func TestValidatePassword(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidatePassword("достаточно_длинный"))

	for _, bad := range []string{"", "корот", "short", strings.Repeat("a", MaxPasswordLenBytes+1)} {
		err := ValidatePassword(bad)
		require.Errorf(t, err, "пароль %q должен быть отклонён", bad)

		var validationErr ValidationError
		require.ErrorAs(t, err, &validationErr)
		require.Equal(t, "password", validationErr.Field)
	}
}

func TestValidatePasswordПринимаетРовноМинимум(t *testing.T) {
	t.Parallel()
	require.NoError(t, ValidatePassword(strings.Repeat("a", MinPasswordLen)))
}

func TestValidatePasswordСчитаетСимволыАНеБайты(t *testing.T) {
	t.Parallel()

	// 8 кириллических символов — это 16 байт, но для пользователя это
	// ровно 8 символов, поэтому пароль должен приниматься.
	require.NoError(t, ValidatePassword("пароль12"))
	// 7 символов — уже короткий.
	require.Error(t, ValidatePassword("пароль1"))
}
