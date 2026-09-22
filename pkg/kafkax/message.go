// Package kafkax — тонкая обёртка над segmentio/kafka-go: продюсер,
// консьюмер с ручным коммитом offset, ретраями и dead-letter topic,
// а также создание топиков.
package kafkax

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// Заголовки, которыми помечаются сообщения, уехавшие в DLQ.
const (
	HeaderEventID       = "event-id"
	HeaderEventType     = "event-type"
	HeaderOriginalTopic = "x-original-topic"
	HeaderOriginalPart  = "x-original-partition"
	HeaderOriginalOff   = "x-original-offset"
	HeaderError         = "x-error"
	HeaderAttempts      = "x-attempts"
	HeaderFailedAt      = "x-failed-at"
)

// Message — сообщение для публикации.
type Message struct {
	Topic   string
	Key     string
	Value   []byte
	Headers map[string]string
}

// Inbound — принятое сообщение, переданное обработчику.
type Inbound struct {
	Topic     string
	Partition int
	Offset    int64
	Key       string
	Value     []byte
	Headers   map[string]string
	Timestamp time.Time
}

func toKafkaHeaders(h map[string]string) []kafka.Header {
	if len(h) == 0 {
		return nil
	}
	out := make([]kafka.Header, 0, len(h))
	for k, v := range h {
		out = append(out, kafka.Header{Key: k, Value: []byte(v)})
	}
	return out
}

func fromKafkaHeaders(h []kafka.Header) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(h))
	for _, header := range h {
		out[header.Key] = string(header.Value)
	}
	return out
}
