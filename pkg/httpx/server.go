package httpx

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// Server — HTTP-сервер с корректным завершением по отмене контекста.
type Server struct {
	srv             *http.Server
	log             *slog.Logger
	shutdownTimeout time.Duration
}

// ServerConfig описывает параметры сервера.
type ServerConfig struct {
	Addr            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
}

// NewServer собирает сервер поверх готового handler.
func NewServer(cfg ServerConfig, handler http.Handler, log *slog.Logger) *Server {
	return &Server{
		srv: &http.Server{
			Addr:              cfg.Addr,
			Handler:           handler,
			ReadTimeout:       cfg.ReadTimeout,
			ReadHeaderTimeout: cfg.ReadTimeout,
			WriteTimeout:      cfg.WriteTimeout,
			IdleTimeout:       cfg.IdleTimeout,
		},
		log:             log,
		shutdownTimeout: cfg.ShutdownTimeout,
	}
}

// Run запускает сервер и блокируется до отмены ctx.
// После отмены даёт активным запросам доработать в пределах shutdownTimeout.
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)

	go func() {
		s.log.Info("http-сервер запущен", slog.String("addr", s.srv.Addr))
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	s.log.Info("http-сервер: начато корректное завершение")

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.shutdownTimeout)
	defer cancel()

	if err := s.srv.Shutdown(shutdownCtx); err != nil {
		// Дожидаться нечего: закрываем принудительно, чтобы не зависнуть.
		_ = s.srv.Close()
		return err
	}

	s.log.Info("http-сервер остановлен")
	return <-errCh
}
