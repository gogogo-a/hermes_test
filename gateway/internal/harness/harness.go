package harness

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"hermes/gateway/internal/model"
)

var ErrMaxHopsExceeded = errors.New("max hops exceeded")

// CheckEnvelope blocks runs that violate hop limits before producing to workload topics.
func CheckEnvelope(ev *model.TaskEnvelope, maxHops int) error {
	if maxHops < 0 {
		maxHops = 0
	}
	if ev.Hop > maxHops {
		return ErrMaxHopsExceeded
	}
	return nil
}

// DLQFrom builds a diagnostics envelope for DLQ topic.
func DLQFrom(cfgMaxHops int, reason string, original *model.TaskEnvelope) model.TaskEnvelope {
	cid := "unknown-correlation-id"
	pid := ""
	if original != nil {
		cid = original.CorrelationID
		pid = original.TaskID
	}
	payload := map[string]any{
		"reason":        reason,
		"configured_max_hops": cfgMaxHops,
		"captured_at":   time.Now().UTC().Format(time.RFC3339Nano),
	}
	if original != nil {
		payload["original_envelope_json_hint"] = map[string]any{
			"task_id":         original.TaskID,
			"agent":           original.Agent,
			"hop":             original.Hop,
			"correlation_id":  original.CorrelationID,
		}
	}
	return model.TaskEnvelope{
		TaskID:          mustUUID(),
		CorrelationID:   cid,
		ParentTaskID:    strPtr(pid),
		Agent:           "system.dlq",
		SchemaVersion:   "1",
		Hop:             cfgMaxHops + 1,
		Payload:         payload,
	}
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func mustUUID() string {
	return uuid.New().String()
}
