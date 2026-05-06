from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

load_dotenv()

from hermes_sdk.util import child_envelope, env

_ALLOWED_AGENTS = frozenset({"agent.copy", "agent.research", "rag.retrieve"})


def _normalize_agent(name: str) -> str:
    n = name.strip().lower()
    if n in _ALLOWED_AGENTS:
        return n
    alias = {"copy": "agent.copy", "research": "agent.research", "rag": "rag.retrieve", "rag.retrieve": "rag.retrieve"}
    return alias.get(n, "agent.research")


def _ensure_min_subtasks(user_message: str, subtasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for st in subtasks:
        agent = _normalize_agent(str(st.get("agent", "")))
        instructions = str(st.get("instructions", "")).strip()
        args = st.get("args") if isinstance(st.get("args"), dict) else {}
        out.append({"agent": agent, "instructions": instructions or user_message, "args": args})
    if len(out) < 2:
        out.append(
            {
                "agent": "agent.copy",
                "instructions": f"补充：根据需求生成一句中文营销短句：{user_message}",
                "args": {},
            }
        )
    return out


def _producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=env("KAFKA_BOOTSTRAP").split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda v: b"" if v is None else str(v).encode("utf-8"),
    )


def _consumer() -> KafkaConsumer:
    return KafkaConsumer(
        env("TOPIC_TASKS_INBOUND", "hermes.tasks.inbound"),
        bootstrap_servers=env("KAFKA_BOOTSTRAP").split(","),
        group_id=env("ROUTER_GROUP_ID", "hermes-router"),
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )


def _plan_with_glm(user_message: str) -> List[Dict[str, Any]]:
    from zhipuai import ZhipuAI

    client = ZhipuAI(api_key=env("ZHIPU_API_KEY"))
    system = (
        "你是 Hermes 风格的路由主控。只输出 JSON，不要 Markdown。"
        "Schema: {\"subtasks\":[{\"agent\":\"agent.copy|agent.research|rag.retrieve\","
        "\"instructions\":string,\"args\":object}]}. "
        "至少产生 2 个子任务。args 可选；若需知识检索，用 rag.retrieve 并给出 query。"
    )
    user = f"用户输入：{user_message}"
    resp = client.chat.completions.create(
        model=os.environ.get("ZHIPU_ROUTER_MODEL", "glm-4-flash"),
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.2,
        response_format={"type": "json_object"},
    )
    content = resp.choices[0].message.content
    data = json.loads(content)
    tasks = data.get("subtasks") or []
    if not isinstance(tasks, list) or len(tasks) < 1:
        raise ValueError("router returned no subtasks")
    return tasks


def run() -> None:
    max_hops = int(os.environ.get("MAX_HOPS", "5"))
    topic_dispatch = env("TOPIC_TASKS_DISPATCH", "hermes.tasks.dispatch")
    topic_dlq = env("TOPIC_TASKS_DLQ", "hermes.tasks.dlq")

    prod = _producer()
    cons = _consumer()

    print("router started", flush=True)
    for msg in cons:
        parent = msg.value
        if not isinstance(parent, dict):
            continue
        if parent.get("agent") != "inbound":
            continue
        user_message = str(parent.get("payload", {}).get("user_message", "")).strip()
        if not user_message:
            continue

        if int(parent.get("hop", 0)) > max_hops:
            dlq = child_envelope(parent, "system.dlq", {"reason": "hop_exceeded_before_router", "parent": parent})
            prod.send(topic_dlq, key=dlq["correlation_id"], value=dlq)
            prod.flush()
            continue

        try:
            subtasks = _ensure_min_subtasks(user_message, _plan_with_glm(user_message))
        except Exception as exc:  # noqa: BLE001
            dlq = child_envelope(
                parent,
                "system.dlq",
                {"reason": "router_failed", "error": str(exc), "parent_task_id": parent.get("task_id")},
            )
            prod.send(topic_dlq, key=dlq["correlation_id"], value=dlq)
            prod.flush()
            time.sleep(0.2)
            continue

        for st in subtasks:
            agent = str(st.get("agent", "")).strip()
            instructions = str(st.get("instructions", "")).strip()
            args = st.get("args") if isinstance(st.get("args"), dict) else {}
            payload = {"instructions": instructions, "args": args, "source": "router"}
            child = child_envelope(parent, agent, payload)
            if int(child.get("hop", 0)) > max_hops:
                dlq = child_envelope(
                    parent,
                    "system.dlq",
                    {"reason": "hop_exceeded_after_plan", "planned_agent": agent},
                )
                prod.send(topic_dlq, key=dlq["correlation_id"], value=dlq)
                continue
            prod.send(topic_dispatch, key=child["correlation_id"], value=child)
        prod.flush()


if __name__ == "__main__":
    run()
