package config

import (
	"github.com/kelseyhightower/envconfig"
)

type Spec struct {
	KafkaBootstrap string `envconfig:"KAFKA_BOOTSTRAP" default:"localhost:19092"`
	GatewayAddr    string `envconfig:"GATEWAY_ADDR" default:":8080"`
	TopicInbound   string `envconfig:"TOPIC_TASKS_INBOUND" default:"hermes.tasks.inbound"`
	TopicDLQ       string `envconfig:"TOPIC_TASKS_DLQ" default:"hermes.tasks.dlq"`
	TaskSchemaPath string `envconfig:"TASK_SCHEMA_PATH" default:"contracts/task-envelope.schema.json"`
	MaxHops        int    `envconfig:"MAX_HOPS" default:"5"`
}

func Load() (Spec, error) {
	var c Spec
	if err := envconfig.Process("", &c); err != nil {
		return Spec{}, err
	}
	return c, nil
}
