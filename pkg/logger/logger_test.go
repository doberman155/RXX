package logger_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hshsb/shop/pkg/logger"
)

func TestNewПишетJSONСИменемСервиса(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	log := logger.NewWithWriter(&buf, "order-service", "info")

	log.Info("заказ создан", slog.String(logger.KeyOrderID, "order-1"))

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry))
	require.Equal(t, "order-service", entry["service"])
	require.Equal(t, "order-1", entry["order_id"])
	require.Equal(t, "заказ создан", entry["msg"])
	require.Equal(t, "INFO", entry["level"])
}

func TestУровеньЛогированияФильтруетЗаписи(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	log := logger.NewWithWriter(&buf, "svc", "warn")

	log.Debug("отладка")
	log.Info("информация")
	require.Empty(t, buf.String())

	log.Warn("предупреждение")
	require.Contains(t, buf.String(), "предупреждение")
}

func TestНеизвестныйУровеньПадаетВInfo(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	log := logger.NewWithWriter(&buf, "svc", "неизвестно")

	log.Info("информация")
	require.Contains(t, buf.String(), "информация")
}

func TestКонтекстПереноситЛоггер(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	log := logger.NewWithWriter(&buf, "svc", "info").With(slog.String("request_id", "req-1"))

	ctx := logger.Into(context.Background(), log)
	logger.From(ctx).Info("обработка")

	require.Contains(t, buf.String(), "req-1")
}

func TestFromБезЛоггераВозвращаетДефолтный(t *testing.T) {
	t.Parallel()
	require.NotNil(t, logger.From(context.Background()))
}

func TestWithOrderIDДобавляетИдентификаторЗаказа(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	log := logger.WithOrderID(logger.NewWithWriter(&buf, "svc", "info"), "order-42")

	log.Info("статус изменён")

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry))
	require.Equal(t, "order-42", entry["order_id"])
}
