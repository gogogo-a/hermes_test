#!/usr/bin/env python3
"""Print up to N JSON messages consumed from tasks.results."""

from __future__ import annotations

import json
import os
import pathlib
from uuid import uuid4

from dotenv import load_dotenv
from kafka import KafkaConsumer

_ROOT = pathlib.Path(__file__).resolve().parents[1]
load_dotenv(_ROOT / ".env")


def main() -> None:
    bootstrap = os.environ["KAFKA_BOOTSTRAP"]
    topic = os.environ.get("TOPIC_TASKS_RESULTS", "hermes.tasks.results")
    limit = int(os.environ.get("SMOKE_READ_LIMIT", "20"))
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[b.strip() for b in bootstrap.split(",") if b.strip()],
        group_id=os.environ.get("DUMP_GROUP_ID", f"dump-{uuid4()}"),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=4000,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    seen = 0
    try:
        for msg in consumer:
            print(json.dumps(msg.value, ensure_ascii=False))
            seen += 1
            if seen >= limit:
                break
    finally:
        consumer.close()

    print(f"-- done, messages={seen} --")


if __name__ == "__main__":
    main()
