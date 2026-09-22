// Package platform содержит общий каркас запуска сервиса: обработку
// сигналов, параллельный запуск компонентов и graceful shutdown.
package platform

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

// Component — фоновая часть сервиса: HTTP-сервер, консьюмер Kafka,
// релеер outbox. Run обязан завершиться после отмены ctx.
type Component struct {
	Name string
	Run  func(ctx context.Context) error
}

// Run запускает все компоненты и блокируется до SIGTERM/SIGINT либо до
// первой ошибки. После сигнала компоненты получают отмену контекста:
// они перестают принимать новую работу, дорабатывают текущую и закрывают
// соединения. Если уложиться в shutdownTimeout не удалось, возвращается ошибка.
func Run(ctx context.Context, log *slog.Logger, shutdownTimeout time.Duration, components ...Component) error {
	if len(components) == 0 {
		return fmt.Errorf("platform: нет компонентов для запуска")
	}

	signalCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	group, groupCtx := errgroup.WithContext(signalCtx)

	for _, c := range components {
		group.Go(func() error {
			log.Info("компонент запускается", slog.String("component", c.Name))
			if err := c.Run(groupCtx); err != nil {
				return fmt.Errorf("компонент %s: %w", c.Name, err)
			}
			log.Info("компонент завершён", slog.String("component", c.Name))
			return nil
		})
	}

	// Ждём завершения всех компонентов, но не дольше shutdownTimeout после
	// получения сигнала: зависший компонент не должен блокировать выход.
	done := make(chan error, 1)
	go func() { done <- group.Wait() }()

	select {
	case err := <-done:
		return err
	case <-signalCtx.Done():
		log.Info("получен сигнал остановки, начинается корректное завершение",
			slog.Duration("timeout", shutdownTimeout))
	}

	select {
	case err := <-done:
		if err != nil {
			return err
		}
		log.Info("сервис остановлен корректно")
		return nil
	case <-time.After(shutdownTimeout):
		return fmt.Errorf("platform: компоненты не остановились за %s", shutdownTimeout)
	}
}
