package model

// TaskEnvelope mirrors contracts/task-envelope.schema.json for JSON encode/decode.
type TaskEnvelope struct {
	TaskID          string                 `json:"task_id"`
	CorrelationID   string                 `json:"correlation_id"`
	ParentTaskID    *string                `json:"parent_task_id,omitempty"`
	Trace           map[string]any          `json:"trace,omitempty"`
	Agent           string                 `json:"agent"`
	SchemaVersion   string                 `json:"schema_version"`
	Hop             int                    `json:"hop"`
	Iteration       *int                   `json:"iteration,omitempty"`
	TTLUnix         *int64                 `json:"ttl_unix,omitempty"`
	NextAgent       *string                `json:"next_agent,omitempty"`
	Payload         map[string]any         `json:"payload"`
}
