package auth

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"time"
)

// newTokenID формирует уникальный jti, чтобы два токена, выпущенных в одну
// секунду, отличались. Случайности достаточно 8 байт.
func newTokenID(now time.Time) string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand на практике не отказывает; фолбэк на наносекунды,
		// чтобы выпуск токена не падал.
		return strconv.FormatInt(now.UnixNano(), 36)
	}
	return hex.EncodeToString(buf)
}
