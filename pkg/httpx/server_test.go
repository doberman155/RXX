package httpx_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hshsb/shop/pkg/httpx"
	"github.com/hshsb/shop/pkg/logger"
)

// freePort занимает свободный порт и сразу его отпускает.
func freePort(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())

	return fmt.Sprintf("%d", port)
}

func TestServerОбслуживаетЗапросыИЗавершаетсяКорректно(t *testing.T) {
	port := freePort(t)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", httpx.Health("test-service"))

	srv := httpx.NewServer(httpx.ServerConfig{
		Addr:            ":" + port,
		ReadTimeout:     2 * time.Second,
		WriteTimeout:    5 * time.Second,
		IdleTimeout:     5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	}, mux, logger.NewWithWriter(io.Discard, "test", "error"))

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	url := "http://127.0.0.1:" + port + "/healthz"
	require.Eventually(t, func() bool {
		resp, err := http.Get(url)
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 50*time.Millisecond)

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("сервер не остановился по отмене контекста")
	}

	_, err := http.Get(url)
	require.Error(t, err, "после остановки сервер не должен принимать запросы")
}

func TestServerДорабатываетТекущийЗапросПриОстановке(t *testing.T) {
	port := freePort(t)

	started := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /slow", func(w http.ResponseWriter, r *http.Request) {
		close(started)
		// Запрос, начатый до сигнала остановки, должен быть доработан.
		time.Sleep(700 * time.Millisecond)
		_, _ = io.WriteString(w, "готово")
	})

	srv := httpx.NewServer(httpx.ServerConfig{
		Addr:            ":" + port,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    10 * time.Second,
		IdleTimeout:     10 * time.Second,
		ShutdownTimeout: 10 * time.Second,
	}, mux, logger.NewWithWriter(io.Discard, "test", "error"))

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	url := "http://127.0.0.1:" + port + "/slow"
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:"+port, 200*time.Millisecond)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 5*time.Second, 50*time.Millisecond)

	type result struct {
		body string
		err  error
	}
	respCh := make(chan result, 1)

	go func() {
		resp, err := http.Get(url)
		if err != nil {
			respCh <- result{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()

		body, err := io.ReadAll(resp.Body)
		respCh <- result{body: string(body), err: err}
	}()

	<-started
	cancel() // остановка сервиса в момент обработки запроса

	select {
	case res := <-respCh:
		require.NoError(t, res.err)
		require.Equal(t, "готово", res.body, "начатый запрос должен быть доработан")
	case <-time.After(10 * time.Second):
		t.Fatal("ответ на начатый запрос не получен")
	}

	require.NoError(t, <-done)
}

func TestServerВозвращаетОшибкуЗанятогоПорта(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	port := fmt.Sprintf("%d", ln.Addr().(*net.TCPAddr).Port)

	srv := httpx.NewServer(httpx.ServerConfig{
		Addr:            "127.0.0.1:" + port,
		ShutdownTimeout: time.Second,
	}, http.NewServeMux(), logger.NewWithWriter(io.Discard, "test", "error"))

	require.Error(t, srv.Run(context.Background()))
}
