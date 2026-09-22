package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// loader собирает значения из переменных окружения и накапливает ошибки,
// чтобы сервис на старте сообщал сразу обо всех проблемах конфигурации.
type loader struct {
	errs []string
}

func (l *loader) fail(key, reason string) {
	l.errs = append(l.errs, fmt.Sprintf("%s: %s", key, reason))
}

func (l *loader) err() error {
	if len(l.errs) == 0 {
		return nil
	}
	return fmt.Errorf("некорректная конфигурация: %s", strings.Join(l.errs, "; "))
}

// str возвращает значение переменной или def, если она не задана.
func (l *loader) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// required возвращает значение обязательной переменной окружения.
func (l *loader) required(key string) string {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		l.fail(key, "обязательная переменная не задана")
		return ""
	}
	return v
}

func (l *loader) secret(key string, minLen int) string {
	v := l.required(key)
	if v != "" && len(v) < minLen {
		l.fail(key, fmt.Sprintf("секрет короче %d символов", minLen))
	}
	return v
}

func (l *loader) duration(key string, def time.Duration) time.Duration {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		l.fail(key, fmt.Sprintf("ожидается длительность (например 500ms, 10s), получено %q", raw))
		return def
	}
	if v <= 0 {
		l.fail(key, "длительность должна быть положительной")
		return def
	}
	return v
}

func (l *loader) intVal(key string, def int) int {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		l.fail(key, fmt.Sprintf("ожидается целое число, получено %q", raw))
		return def
	}
	return v
}

func (l *loader) positiveInt(key string, def int) int {
	v := l.intVal(key, def)
	if v <= 0 {
		l.fail(key, "значение должно быть больше нуля")
		return def
	}
	return v
}

func (l *loader) boolVal(key string, def bool) bool {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		l.fail(key, fmt.Sprintf("ожидается true/false, получено %q", raw))
		return def
	}
	return v
}

// csv разбирает список значений через запятую (брокеры Kafka и т.п.).
func (l *loader) csv(key string, def []string) []string {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return def
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		l.fail(key, "список пуст")
		return def
	}
	return out
}
