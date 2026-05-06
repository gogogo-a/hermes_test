package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"hermes/gateway/internal/model"
)

type Producer struct {
	bootstrap string
	writer    *kafka.Writer
}

func NewProducer(bootstrap string) *Producer {
	brokers := splitBrokers(bootstrap)
	return &Producer{
		bootstrap: bootstrap,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
			BatchTimeout: 10 * time.Millisecond,
		},
	}
}

func splitBrokers(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return []string{"localhost:19092"}
	}
	return out
}

func (p *Producer) Publish(ctx context.Context, topic string, key string, ev *model.TaskEnvelope) error {
	bs, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: bs,
		Time:  time.Now().UTC(),
	}
	return p.writer.WriteMessages(ctx, msg)
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

// NewInboundEnvelope builds the root task for a natural language user message.
func NewInboundEnvelope(message string) model.TaskEnvelope {
	cid := uuid.New().String()
	return model.TaskEnvelope{
		TaskID:        uuid.New().String(),
		CorrelationID: cid,
		Agent:         "inbound",
		SchemaVersion: "1",
		Hop:           0,
		Payload: map[string]any{
			"user_message": message,
		},
	}
}
