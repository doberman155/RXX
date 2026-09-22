package kafkax

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/hshsb/shop/pkg/config"
	"github.com/hshsb/shop/pkg/logger"
)

// Handler обрабатывает одно сообщение. Возврат ошибки означает повтор,
// а после исчерпания попыток — отправку в DLQ.
type Handler func(ctx context.Context, msg Inbound) error

// ErrNonRetryable помечает ошибки, которые повторять бессмысленно
// (например, битый JSON): такое сообщение сразу уходит в DLQ.
var ErrNonRetryable = errors.New("сообщение не подлежит повторной обработке")

// NonRetryable оборачивает ошибку как неповторяемую.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrNonRetryable, err.Error())
}

// ConsumerOptions описывает консьюмер одной consumer-группы.
type ConsumerOptions struct {
	Topics []string
	// GroupID — своя группа у каждого консьюмера каждого сервиса.
	GroupID string
	// ProcessTimeout ограничивает обработку одного сообщения.
	ProcessTimeout time.Duration
}

// Consumer читает сообщения из Kafka в рамках consumer-группы и коммитит
// offset только после успешной обработки (или отправки в DLQ).
type Consumer struct {
	reader         *kafka.Reader
	dlq            *Producer
	handler        Handler
	log            *slog.Logger
	groupID        string
	topics         []string
	maxRetries     int
	backoff        time.Duration
	maxBackoff     time.Duration
	dlqSuffix      string
	processTimeout time.Duration
}

// NewConsumer собирает консьюмер. dlq — продюсер для dead-letter topic.
func NewConsumer(cfg config.Kafka, opts ConsumerOptions, handler Handler, dlq *Producer, log *slog.Logger) (*Consumer, error) {
	if len(opts.Topics) == 0 {
		return nil, errors.New("kafka: не задан список топиков консьюмера")
	}
	if opts.GroupID == "" {
		return nil, errors.New("kafka: не задан group id консьюмера")
	}
	if handler == nil {
		return nil, errors.New("kafka: не задан обработчик сообщений")
	}
	if dlq == nil {
		return nil, errors.New("kafka: не задан продюсер dead-letter topic")
	}

	processTimeout := opts.ProcessTimeout
	if processTimeout <= 0 {
		processTimeout = 30 * time.Second
	}

	groupLog := log.With(slog.String("consumer_group", opts.GroupID))

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		GroupID:     opts.GroupID,
		GroupTopics: opts.Topics,
		MinBytes:    cfg.MinBytes,
		MaxBytes:    cfg.MaxBytes,
		// CommitInterval = 0 — коммит только явным вызовом CommitMessages.
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
		MaxWait:        time.Second,
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...any) {
			groupLog.Error("kafka-reader: " + fmt.Sprintf(msg, args...))
		}),
	})

	return &Consumer{
		reader:         reader,
		dlq:            dlq,
		handler:        handler,
		log:            groupLog,
		groupID:        opts.GroupID,
		topics:         opts.Topics,
		maxRetries:     cfg.MaxRetries,
		backoff:        cfg.RetryBackoff,
		maxBackoff:     cfg.MaxRetryBackoff,
		dlqSuffix:      cfg.DLQSuffix,
		processTimeout: processTimeout,
	}, nil
}

// Run читает сообщения до отмены контекста.
// При отмене новые сообщения не забираются, текущее дорабатывается и
// коммитится, после чего соединение закрывается.
func (c *Consumer) Run(ctx context.Context) error {
	c.log.Info("консьюмер запущен", slog.Any("topics", c.topics))

	defer func() {
		if err := c.reader.Close(); err != nil {
			c.log.Error("не удалось закрыть kafka-reader", slog.String("error", err.Error()))
		} else {
			c.log.Info("консьюмер остановлен")
		}
	}()

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			switch {
			case ctx.Err() != nil:
				return nil
			case errors.Is(err, context.Canceled), errors.Is(err, io.EOF):
				return nil
			default:
				return fmt.Errorf("получить сообщение: %w", err)
			}
		}

		if err := c.process(ctx, msg); err != nil {
			return err
		}

		// Завершаемся только после того, как текущее сообщение доработано.
		if ctx.Err() != nil {
			return nil
		}
	}
}

// process обрабатывает одно сообщение с ретраями и коммитит offset.
func (c *Consumer) process(ctx context.Context, msg kafka.Message) error {
	inbound := Inbound{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       string(msg.Key),
		Value:     msg.Value,
		Headers:   fromKafkaHeaders(msg.Headers),
		Timestamp: msg.Time,
	}

	msgLog := c.log.With(
		slog.String(logger.KeyTopic, msg.Topic),
		slog.Int("partition", msg.Partition),
		slog.Int64("offset", msg.Offset),
		slog.String(logger.KeyOrderID, inbound.Key),
	)

	handlerErr := c.handleWithRetries(ctx, inbound, msgLog)

	// Обработку прервали из-за остановки сервиса: offset не коммитим,
	// сообщение будет доставлено повторно (обработка идемпотентна).
	if handlerErr != nil && ctx.Err() != nil && !errors.Is(handlerErr, ErrNonRetryable) {
		msgLog.Warn("обработка прервана остановкой сервиса, offset не закоммичен",
			slog.String("error", handlerErr.Error()))
		return nil
	}

	if handlerErr != nil {
		if err := c.sendToDLQ(ctx, inbound, handlerErr, msgLog); err != nil {
			// DLQ недоступна — останавливаем консьюмер, чтобы не терять сообщения.
			return fmt.Errorf("отправить сообщение в DLQ: %w", err)
		}
	}

	// Коммит выполняем на контексте, переживающем отмену: иначе offset
	// уже обработанного сообщения не сохранится.
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	if err := c.reader.CommitMessages(commitCtx, msg); err != nil {
		return fmt.Errorf("закоммитить offset: %w", err)
	}
	return nil
}

// handleWithRetries вызывает обработчик, повторяя попытки с нарастающей задержкой.
func (c *Consumer) handleWithRetries(ctx context.Context, msg Inbound, msgLog *slog.Logger) error {
	backoff := c.backoff
	var lastErr error

	for attempt := 1; attempt <= c.maxRetries; attempt++ {
		// Обработка не прерывается отменой ctx: текущее сообщение нужно доработать.
		procCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.processTimeout)
		procCtx = logger.Into(procCtx, msgLog)

		err := c.handler(procCtx, msg)
		cancel()

		if err == nil {
			if attempt > 1 {
				msgLog.Info("сообщение обработано после повторов", slog.Int("attempts", attempt))
			}
			return nil
		}

		lastErr = err

		if errors.Is(err, ErrNonRetryable) {
			msgLog.Error("сообщение не подлежит повтору, отправляем в DLQ",
				slog.String("error", err.Error()))
			return err
		}

		if attempt == c.maxRetries {
			break
		}

		msgLog.Warn("ошибка обработки сообщения, будет повтор",
			slog.Int("attempt", attempt),
			slog.Duration("backoff", backoff),
			slog.String("error", err.Error()),
		)

		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(backoff):
		}

		if backoff *= 2; backoff > c.maxBackoff {
			backoff = c.maxBackoff
		}
	}

	msgLog.Error("исчерпаны попытки обработки, сообщение отправляется в DLQ",
		slog.Int("attempts", c.maxRetries),
		slog.String("error", lastErr.Error()),
	)
	return lastErr
}

// sendToDLQ публикует исходное сообщение в <topic>.dlq вместе с причиной отказа.
func (c *Consumer) sendToDLQ(ctx context.Context, msg Inbound, cause error, msgLog *slog.Logger) error {
	headers := make(map[string]string, len(msg.Headers)+6)
	for k, v := range msg.Headers {
		headers[k] = v
	}
	headers[HeaderOriginalTopic] = msg.Topic
	headers[HeaderOriginalPart] = strconv.Itoa(msg.Partition)
	headers[HeaderOriginalOff] = strconv.FormatInt(msg.Offset, 10)
	headers[HeaderError] = cause.Error()
	headers[HeaderAttempts] = strconv.Itoa(c.maxRetries)
	headers[HeaderFailedAt] = time.Now().UTC().Format(time.RFC3339Nano)

	dlqTopic := msg.Topic + c.dlqSuffix

	// Публикуем на контексте, переживающем отмену: сообщение уже вычитано.
	pubCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()

	if err := c.dlq.Publish(pubCtx, Message{
		Topic:   dlqTopic,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	}); err != nil {
		return err
	}

	msgLog.Warn("сообщение отправлено в dead-letter topic", slog.String("dlq_topic", dlqTopic))
	return nil
}
