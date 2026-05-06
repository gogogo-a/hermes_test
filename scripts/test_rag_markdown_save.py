#!/usr/bin/env python3
"""Verify RAG: Markdown body + JSON metadata persist in Qdrant and round-trip on query."""

from __future__ import annotations

import json
import os
import pathlib
import sys
import urllib.error
import urllib.request


def load_env(root: pathlib.Path) -> None:
    for name in (".env",):
        path = root / name
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def json_post(url: str, payload: dict, timeout: int = 120) -> dict:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def http_get(url: str, timeout: int = 30) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> int:
    root = pathlib.Path(__file__).resolve().parents[1]
    load_env(root)
    base = os.environ.get("RAG_URL", "http://127.0.0.1:8090").rstrip("/")

    doc_id = "md_json_test_doc"
    md = """# 测试文档

## 架构
本项目使用 **Kafka** 分发子任务，Qdrant 存向量。

- 网关: Go
- 大脑: Nous Hermes-Agent + 智谱
"""
    meta = {
        "doc_type": "test",
        "labels": ["kafka", "qdrant"],
        "version": 1,
        "tier": None,
    }

    try:
        before = http_get(f"{base}/internal/stats")
    except urllib.error.HTTPError:
        print("FAIL: GET /internal/stats (is RAG service up?)")
        return 1
    except urllib.error.URLError as exc:
        print(f"FAIL: cannot reach RAG ({base}): {exc}")
        return 1

    ing = json_post(
        f"{base}/internal/ingest",
        {"documents": [{"id": doc_id, "markdown": md, "metadata": meta}]},
    )
    if ing.get("upserted", 0) < 1:
        print("FAIL: ingest upserted=0")
        return 1

    after = http_get(f"{base}/internal/stats")
    if (after.get("points_count") or 0) < (before.get("points_count") or 0):
        print("WARN: points_count did not increase (collection reset?)")

    q_body = json_post(f"{base}/internal/query", {"query": "Kafka 网关 Go", "top_k": 6})
    ctx = q_body.get("context") or []
    if not ctx:
        print("FAIL: query returned empty context")
        return 1

    hit = ctx[0]
    got_meta = hit.get("meta") or {}
    nested = got_meta.get("metadata")
    md_snip = (hit.get("text") or "").strip()

    if not isinstance(nested, dict):
        print(f"FAIL: metadata not dict: {nested!r}")
        return 1
    if nested.get("doc_type") != "test":
        print(f"FAIL: unexpected metadata: {nested}")
        return 1
    if nested.get("labels") != ["kafka", "qdrant"]:
        print(f"FAIL: labels mismatch: {nested.get('labels')}")
        return 1
    if "Kafka" not in md_snip:
        print(f"FAIL: markdown chunk missing Kafka: {md_snip[:120]!r}")
        return 1

    print("OK ingest+query Markdown+JSON metadata round-trip.")
    print(f"  upserted={ing['upserted']} points_before={before.get('points_count')} after={after.get('points_count')}")
    print(f"  top doc_id={got_meta.get('doc_id')} doc_type={nested.get('doc_type')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
