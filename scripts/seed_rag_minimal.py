#!/usr/bin/env python3
import json
import os
import pathlib
import urllib.request


def load_env(path: pathlib.Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


def main() -> None:
    root = pathlib.Path(__file__).resolve().parents[1]
    load_env(root / ".env")
    rag = os.environ.get("RAG_URL", "http://127.0.0.1:8090")

    texts = os.environ.get("SEED_TEXT")
    docs = []

    readme = root / "README.md"
    if readme.exists():
        docs.append(
            {
                "id": "readme",
                "markdown": readme.read_text(encoding="utf-8", errors="ignore"),
                "metadata": {"source": "README.md", "doc_type": "project_doc"},
            }
        )

    docs.append(
        {
            "id": "local_note",
            "markdown": texts or "Hermes Kafka 流水线：路由把需求拆为多子任务并由 worker 异步执行。",
            "metadata": {"source": "seed", "doc_type": "note"},
        }
    )

    req = urllib.request.Request(
        f"{rag.rstrip('/')}/internal/ingest",
        data=json.dumps({"documents": docs}, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        body = resp.read().decode("utf-8")
    print(body)


if __name__ == "__main__":
    main()
