"""
Adapter: use NousResearch ``hermes-agent`` (run_agent.AIAgent) as the Kafka router brain.

智谱开放平台 Key 沿用项目 ``ZHIPU_API_KEY``，在导入 ``run_agent`` 前映射为
``ZAI_API_KEY`` / ``GLM_API_KEY``，并用 OpenAI 兼容端点 ``GLM_BASE_URL``（默认
国内 ``open.bigmodel.cn``）。

文档: https://github.com/NousResearch/hermes-agent
"""

from __future__ import annotations

import json
import os
import pathlib
import re
from typing import Any, Dict, List

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
_NOS_HOME = _REPO_ROOT / ".hermes_nous_home"

_ROUTER_EPHEMERAL = (
    "你是 Hermes-Agent 范式下的路由主脑，当前任务只做「拆分下游子任务」，不调用工具。\n"
    "约束：回复必须是 **唯一一段合法 JSON 对象**，不要 Markdown、不要用代码围栏、不要追加解释。\n"
    "Schema 严格如下：\n"
    '{"subtasks":[{"agent":"agent.copy|agent.research|rag.retrieve","instructions":"string","args":{} }]}\n'
    "至少 2 个子任务。需要查知识时用 rag.retrieve，并在 args 里给 {\"query\": \"...\"}。\n"
    "agent 只允许上述三个字面量之一。"
)


def _bootstrap_nous_runtime() -> None:
    """Must run before ``import run_agent`` (Hermes imports load dotenv/home)."""
    _NOS_HOME.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("HERMES_HOME", str(_NOS_HOME))
    os.environ.setdefault("PYTHONUTF8", "1")

    key = (
        os.environ.get("ZHIPU_API_KEY", "").strip()
        or os.environ.get("ZHIPUAI_API_KEY", "").strip()
        or os.environ.get("ZAI_API_KEY", "").strip()
        or os.environ.get("GLM_API_KEY", "").strip()
    )
    if key:
        os.environ.setdefault("ZAI_API_KEY", key)
        os.environ.setdefault("GLM_API_KEY", key)

    os.environ.setdefault(
        "GLM_BASE_URL",
        os.environ.get("BIGMODEL_BASE_URL", "https://open.bigmodel.cn/api/paas/v4"),
    )


def _extract_json_object(text: str) -> Dict[str, Any]:
    text = text.strip()
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", text, re.IGNORECASE)
    if fence:
        text = fence.group(1).strip()
    if not text.startswith("{"):
        i = text.find("{")
        if i >= 0:
            text = text[i:]
    j = text.rfind("}")
    if j >= 0:
        text = text[: j + 1]
    return json.loads(text)


_agent_singleton: Any = None


def _get_router_agent():  # noqa: ANN202
    global _agent_singleton
    if _agent_singleton is not None:
        return _agent_singleton

    _bootstrap_nous_runtime()
    from run_agent import AIAgent  # heavyweight import after env

    model = os.environ.get("HERMES_NOUS_MODEL", "glm-4-flash").strip()
    api_key = os.environ.get("ZAI_API_KEY", "").strip() or os.environ.get("GLM_API_KEY", "").strip()
    base_url = os.environ.get("GLM_BASE_URL", "https://open.bigmodel.cn/api/paas/v4").strip().rstrip("/")

    if not api_key:
        raise RuntimeError("missing API key: set ZHIPU_API_KEY (or ZAI_API_KEY / GLM_API_KEY)")

    max_iters = int(os.environ.get("HERMES_NOUS_MAX_ITERATIONS", "12"))

    _agent_singleton = AIAgent(
        base_url=base_url,
        api_key=api_key,
        provider="zai",
        model=model,
        max_iterations=max_iters,
        enabled_toolsets=[],
        quiet_mode=True,
        skip_context_files=True,
        skip_memory=True,
        ephemeral_system_prompt=_ROUTER_EPHEMERAL,
        platform=os.environ.get("HERMES_ROUTER_PLATFORM", "cli"),
        tool_delay=float(os.environ.get("HERMES_NOUS_TOOL_DELAY", "0")),
    )
    return _agent_singleton


def plan_subtasks_with_nous(user_message: str) -> List[Dict[str, Any]]:
    """Run one Nous Hermes-Agent turn (no tools) and parse routing JSON."""
    mock = os.environ.get("HERMES_ROUTER_MOCK", "0").strip().lower()
    if mock in {"1", "true", "yes"}:
        q = user_message.strip().replace("\n", " ")[:200]
        return [
            {
                "agent": "agent.copy",
                "instructions": f"根据需求写一段中文短文案：{q}",
                "args": {},
            },
            {
                "agent": "agent.research",
                "instructions": f"列出与该需求相关的核验要点：{q}",
                "args": {},
            },
        ]

    agent = _get_router_agent()
    result = agent.run_conversation(user_message.strip())
    if not isinstance(result, dict):
        raise ValueError(f"unexpected run_conversation return: {type(result)}")
    final = result.get("final_response") or ""
    if not isinstance(final, str):
        final = json.dumps(final, ensure_ascii=False)
    data = _extract_json_object(final)
    tasks = data.get("subtasks") or []
    if not isinstance(tasks, list) or len(tasks) < 1:
        raise ValueError("router returned no subtasks")
    return tasks
