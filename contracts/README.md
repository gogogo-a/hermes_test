# Contracts

## TaskEnvelope (`task-envelope.schema.json`)

Shared JSON Schema for payloads produced/consumed over Kafka and validated at the gateway.

### Topics

| Topic | Producer | Consumer | Notes |
|-------|----------|----------|-------|
| `hermes.tasks.inbound` | Go gateway | Python router | User intent enters here |
| `hermes.tasks.dispatch` | Router / agents | Workers | Specialized work units |
| `hermes.tasks.results` | Agents | Debugging / aggregator | Outputs |
| `hermes.tasks.dlq` | Gateway / Router / Workers | Operational | Harness violations |

### Harness / DLQ

- **`hop`** counts orchestration hops. Environments should cap hops (gateway default **5**) before emitting to **`hermes.tasks.dlq`** with `payload.reason`.
- Prefer **`correlation_id`** for tracing multiple `task_id` values spawned from one user request.
