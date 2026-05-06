package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"hermes/gateway/internal/config"
	"hermes/gateway/internal/harness"
	kafkapkg "hermes/gateway/internal/kafka"
	"hermes/gateway/internal/model"
	"hermes/gateway/internal/schema"
)

type Server struct {
	cfg  config.Spec
	val  *schema.Validator
	prod *kafkapkg.Producer
	log  *log.Logger
}

func New(cfg config.Spec, val *schema.Validator, prod *kafkapkg.Producer, lg *log.Logger) *Server {
	if lg == nil {
		lg = log.Default()
	}
	return &Server{cfg: cfg, val: val, prod: prod, log: lg}
}

func (s *Server) Routes() http.Handler {
	r := chi.NewRouter()
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	r.Post("/api/v1/tasks", s.handleCreateTask)
	return r
}

type createTaskSimple struct {
	Message string `json:"message"`
}

func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}

	var envelope model.TaskEnvelope
	switch {
	case jsonLooksLikeEnvelope(body):
		if err := json.Unmarshal(body, &envelope); err != nil {
			http.Error(w, "invalid envelope JSON", http.StatusBadRequest)
			return
		}
	default:
		var simple createTaskSimple
		if err := json.Unmarshal(body, &simple); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		if strings.TrimSpace(simple.Message) == "" {
			http.Error(w, "`message` required unless posting full envelope", http.StatusBadRequest)
			return
		}
		envelope = kafkapkg.NewInboundEnvelope(strings.TrimSpace(simple.Message))
	}

	if err := harness.CheckEnvelope(&envelope, s.cfg.MaxHops); err != nil {
		if errors.Is(err, harness.ErrMaxHopsExceeded) {
			dlq := harness.DLQFrom(s.cfg.MaxHops, "hop_exceeded_at_gateway", &envelope)
			_ = s.prod.Publish(ctx, s.cfg.TopicDLQ, dlq.CorrelationID, &dlq)
			http.Error(w, "hop limit exceeded", http.StatusBadRequest)
			return
		}
		http.Error(w, "harness error", http.StatusInternalServerError)
		return
	}

	if err := s.val.ValidateInterface(&envelope); err != nil {
		s.log.Printf("validation error: %v", err)
		http.Error(w, "schema validation failed", http.StatusUnprocessableEntity)
		return
	}

	key := envelope.CorrelationID
	if strings.TrimSpace(key) == "" {
		key = envelope.TaskID
	}
	if err := s.prod.Publish(ctx, s.cfg.TopicInbound, key, &envelope); err != nil {
		s.log.Printf("kafka publish: %v", err)
		http.Error(w, "failed to enqueue", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":         "accepted",
		"task_id":        envelope.TaskID,
		"correlation_id": envelope.CorrelationID,
	})
}

func jsonLooksLikeEnvelope(raw []byte) bool {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return false
	}
	_, ok := m["task_id"]
	return ok
}
