// Package httpx содержит общие кирпичики REST-слоя: ответы в JSON,
// middleware (request-id, логирование, recovery, проверка JWT) и
// HTTP-сервер с graceful shutdown.
package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// MaxBodyBytes ограничивает размер тела запроса (1 МиБ).
const MaxBodyBytes = 1 << 20

// Коды ошибок, которые видит клиент. Стабильны и пригодны для обработки на клиенте.
const (
	CodeBadRequest   = "bad_request"
	CodeValidation   = "validation_error"
	CodeUnauthorized = "unauthorized"
	CodeForbidden    = "forbidden"
	CodeNotFound     = "not_found"
	CodeConflict     = "conflict"
	CodeInternal     = "internal_error"
)

// ErrorResponse — единый формат ошибки REST-API.
type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

// ErrorBody — тело ошибки.
type ErrorBody struct {
	Code      string            `json:"code"`
	Message   string            `json:"message"`
	Fields    map[string]string `json:"fields,omitempty"`
	RequestID string            `json:"request_id,omitempty"`
}

// JSON пишет ответ в формате JSON с указанным статусом.
func JSON(w http.ResponseWriter, r *http.Request, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if payload == nil || status == http.StatusNoContent {
		return
	}
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		// Статус уже отправлен, поэтому остаётся только записать это в лог.
		FromRequest(r).Error("не удалось сериализовать ответ", "error", err.Error())
	}
}

// NoContent отвечает 204.
func NoContent(w http.ResponseWriter, r *http.Request) { JSON(w, r, http.StatusNoContent, nil) }

// Error отправляет клиенту ошибку в едином формате.
func Error(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	ErrorWithFields(w, r, status, code, message, nil)
}

// ErrorWithFields отправляет ошибку с пояснением по полям (валидация).
func ErrorWithFields(w http.ResponseWriter, r *http.Request, status int, code, message string, fields map[string]string) {
	JSON(w, r, status, ErrorResponse{Error: ErrorBody{
		Code:      code,
		Message:   message,
		Fields:    fields,
		RequestID: RequestIDFrom(r.Context()),
	}})
}

// DecodeJSON читает тело запроса в dst, запрещая неизвестные поля и
// ограничивая размер тела.
func DecodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		var syntaxErr *json.SyntaxError
		var typeErr *json.UnmarshalTypeError
		var maxBytesErr *http.MaxBytesError

		switch {
		case errors.Is(err, io.EOF):
			return errors.New("тело запроса пустое")
		case errors.Is(err, io.ErrUnexpectedEOF):
			// Тело оборвалось на середине объекта.
			return errors.New("некорректный JSON: тело запроса оборвано")
		case errors.As(err, &syntaxErr):
			return fmt.Errorf("некорректный JSON на позиции %d", syntaxErr.Offset)
		case errors.As(err, &typeErr):
			return fmt.Errorf("поле %q имеет неверный тип", typeErr.Field)
		case errors.As(err, &maxBytesErr):
			return fmt.Errorf("тело запроса больше %d байт", MaxBodyBytes)
		case strings.HasPrefix(err.Error(), "json: unknown field "):
			field := strings.TrimPrefix(err.Error(), "json: unknown field ")
			return fmt.Errorf("неизвестное поле %s", field)
		default:
			return fmt.Errorf("не удалось разобрать тело запроса: %w", err)
		}
	}

	// В теле должен быть ровно один JSON-объект.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("тело запроса должно содержать один JSON-объект")
	}
	return nil
}

// QueryInt читает целочисленный query-параметр с значением по умолчанию.
func QueryInt(r *http.Request, name string, def, minVal, maxVal int) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(name))
	if raw == "" {
		return def, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("параметр %s должен быть целым числом", name)
	}
	if v < minVal || v > maxVal {
		return 0, fmt.Errorf("параметр %s должен быть в диапазоне [%d, %d]", name, minVal, maxVal)
	}
	return v, nil
}
