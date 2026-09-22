package platform_test

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hshsb/shop/internal/platform"
	"github.com/hshsb/shop/pkg/logger"
)

func TestRunТребуетХотяБыОдинКомпонент(t *testing.T) {
	t.Parallel()

	err := platform.Run(context.Background(), logger.NewWithWriter(io.Discard, "test", "error"), time.Second)
	require.Error(t, err)
}

func TestRunОстанавливаетКомпонентыПоОтменеКонтекста(t *testing.T) {
	t.Parallel()

	log := logger.NewWithWriter(io.Discard, "test", "error")

	var started, stopped atomic.Int32
	component := func(name string) platform.Component {
		return platform.Component{
			Name: name,
			Run: func(ctx context.Context) error {
				started.Add(1)
				<-ctx.Done()
				stopped.Add(1)
				return nil
			},
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- platform.Run(ctx, log, 5*time.Second, component("http"), component("consumer"))
	}()

	require.Eventually(t, func() bool { return started.Load() == 2 }, 3*time.Second, 10*time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены контекста")
	}

	require.EqualValues(t, 2, stopped.Load(), "все компоненты должны корректно остановиться")
}

func TestRunВозвращаетОшибкуКомпонента(t *testing.T) {
	t.Parallel()

	log := logger.NewWithWriter(io.Discard, "test", "error")
	boom := errors.New("консьюмер упал")

	var stopped atomic.Bool

	err := platform.Run(context.Background(), log, 5*time.Second,
		platform.Component{Name: "consumer", Run: func(ctx context.Context) error { return boom }},
		platform.Component{Name: "http", Run: func(ctx context.Context) error {
			// Ошибка одного компонента отменяет контекст остальных.
			<-ctx.Done()
			stopped.Store(true)
			return nil
		}},
	)

	require.ErrorIs(t, err, boom)
	require.Contains(t, err.Error(), "consumer")
	require.True(t, stopped.Load(), "остальные компоненты должны быть остановлены")
}

func TestRunСообщаетОЗависшемКомпоненте(t *testing.T) {
	t.Parallel()

	log := logger.NewWithWriter(io.Discard, "test", "error")

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- platform.Run(ctx, log, 200*time.Millisecond,
			platform.Component{Name: "зависший", Run: func(ctx context.Context) error {
				<-release
				return nil
			}},
		)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		require.Contains(t, err.Error(), "не остановились")
	case <-time.After(5 * time.Second):
		t.Fatal("Run должен был завершиться по таймауту")
	}
}
