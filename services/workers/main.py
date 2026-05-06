from __future__ import annotations

import json
import os
import time
from typing import Any, Callable, Dict

import requests
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

load_dotenv()

from hermes_sdk.util import child_envelope, env


Handler = Callable[[Dict[str, Any]], Dict[str, Any]]


def _producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=env("KAFKA_BOOTSTRAP").split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda v: b"" if v is None else str(v).encode("utf-8"),
    )


def _consumer() -> KafkaConsumer:
    return KafkaConsumer(
        env("TOPIC_TASKS_DISPATCH", "hermes.tasks.dispatch"),
        bootstrap_servers=env("KAFKA_BOOTSTRAP").split(","),
        group_id=env("WORKER_GROUP_ID", "hermes-workers"),
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )


def handle_copy(task: Dict[str, Any]) -> Dict[str, Any]:
    instr = str(task.get("payload", {}).get("instructions", ""))
    return {"status": "ok", "artifact": f"【文案草稿】{instr[:500]}"}


def handle_research(task: Dict[str, Any]) -> Dict[str, Any]:
    instr = str(task.get("payload", {}).get("instructions", ""))
    return {"status": "ok", "artifact": f"【要点研究】已整理与「{instr[:120]}」相关的假设与验证思路。"}


def handle_rag(task: Dict[str, Any]) -> Dict[str, Any]:
    base = os.environ.get("RAG_URL", "http://127.0.0.1:8090")
    args = task.get("payload", {}).get("args") or {}
    q = str(args.get("query") or task.get("payload", {}).get("instructions") or "")
    r = requests.post(f"{base.rstrip('/')}/internal/query", json={"query": q, "top_k": 8}, timeout=60)
    r.raise_for_status()
    body = r.json()
    return {"status": "ok", "artifact": body}


def registry() -> Dict[str, Handler]:
    return {
        "agent.copy": handle_copy,
        "agent.research": handle_research,
        "rag.retrieve": handle_rag,
    }


def run() -> None:
    max_hops = int(os.environ.get("MAX_HOPS", "5"))
    topic_results = env("TOPIC_TASKS_RESULTS", "hermes.tasks.results")
    topic_dlq = env("TOPIC_TASKS_DLQ", "hermes.tasks.dlq")
    handlers = registry()

    prod = _producer()
    cons = _consumer()
    print("workers started", flush=True)

    for msg in cons:
        task = msg.value
        if not isinstance(task, dict):
            continue
        agent = str(task.get("agent", ""))
        if agent not in handlers:
            dlq = child_envelope(
                task,
                "system.dlq",
                {"reason": "unknown_agent", "agent": agent},
            )
            prod.send(topic_dlq, key=dlq["correlation_id"], value=dlq)
            prod.flush()
            continue

        if int(task.get("hop", 0)) > max_hops:
            dlq = child_envelope(task, "system.dlq", {"reason": "hop_exceeded_in_worker", "agent": agent})
            prod.send(topic_dlq, key=dlq["correlation_id"], value=dlq)
            prod.flush()
            continue

        try:
            output = handlers[agent](task)
        except Exception as exc:  # noqa: BLE001
            dlq = child_envelope(
                task,
                "system.dlq",
                {"reason": "worker_failed", "agent": agent, "error": str(exc)},
            )
            prod.send(topic_dlq, key=dlq["correlation_id"], value=dlq)
            prod.flush()
            time.sleep(0.1)
            continue

        result = child_envelope(
            task,
            f"{agent}.result",
            {"output": output},
        )
        prod.send(topic_results, key=result["correlation_id"], value=result)
        prod.flush()


if __name__ == "__main__":
    run()
