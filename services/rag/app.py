from __future__ import annotations

import os
import uuid
from typing import Any, Dict, List

import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel, Field
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

from fastembed import TextEmbedding

try:
    from sentence_transformers import CrossEncoder
except Exception:  # noqa: BLE001
    CrossEncoder = None  # type: ignore[misc, assignment]


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _chunk_markdown(md: str, max_len: int = 720, overlap: int = 120) -> List[str]:
    """Slice Markdown without squashing newlines (good enough for vector chunks)."""
    text = md.strip()
    if not text:
        return []
    chunks: List[str] = []
    i = 0
    while i < len(text):
        chunks.append(text[i : i + max_len])
        i += max_len - overlap
    return chunks


class IngestDoc(BaseModel):
    """``markdown`` holds the corpus body; ``metadata`` is JSON-compatible and stored on Qdrant payload."""

    id: str
    markdown: str = ""
    text: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def body(self) -> str:
        raw = self.markdown.strip() or self.text.strip()
        return raw


class IngestRequest(BaseModel):
    documents: List[IngestDoc]


class QueryRequest(BaseModel):
    query: str
    top_k: int = Field(default=8, ge=1, le=32)


app = FastAPI(title="Hermes RAG", version="0.1.0")

_embed_model_name = _env("RAG_FASTEMBED_MODEL", "BAAI/bge-small-zh-v1.5")
_embedder = TextEmbedding(model_name=_embed_model_name)
_dim = len(next(_embedder.embed(["dimension_probe"])))  # resolves real embedding size once
_collection = _env("RAG_COLLECTION", "hermes_docs")
_qdrant_url = _env("QDRANT_URL", "http://127.0.0.1:6333")
_client = QdrantClient(url=_qdrant_url)
_reranker_model = _env("RAG_RERANKER_MODEL", "cross-encoder/ms-marco-MiniLM-L-6-v2")
_reranker = None


def _ensure_collection() -> None:
    cols = _client.get_collections().collections
    names = {c.name for c in cols}
    if _collection not in names:
        _client.create_collection(
            collection_name=_collection,
            vectors_config=VectorParams(size=_dim, distance=Distance.COSINE),
        )


def _get_reranker():
    global _reranker
    if _reranker is not None:
        return _reranker
    if CrossEncoder is None:
        return None
    _reranker = CrossEncoder(_reranker_model)
    return _reranker


def _embed_texts(texts: List[str]) -> List[List[float]]:
    vecs = list(_embedder.embed(texts))
    return [v.tolist() for v in vecs]


@app.on_event("startup")
def _startup() -> None:
    _ensure_collection()


@app.get("/healthz")
def healthz() -> Dict[str, Any]:
    return {"ok": True, "collection": _collection, "embed_model": _embed_model_name}


@app.get("/internal/stats")
def stats() -> Dict[str, Any]:
    _ensure_collection()
    info = _client.get_collection(collection_name=_collection)
    return {"collection": _collection, "points_count": getattr(info, "points_count", None)}


@app.post("/internal/ingest")
def ingest(req: IngestRequest) -> Dict[str, Any]:
    _ensure_collection()
    points: List[PointStruct] = []
    for doc in req.documents:
        body = doc.body()
        chunks = _chunk_markdown(body)
        if not chunks:
            continue
        meta = dict(doc.metadata)
        vectors = _embed_texts(chunks)
        for idx, (chunk, vec) in enumerate(zip(chunks, vectors)):
            pid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{doc.id}:{idx}:{chunk[:96]}"))
            points.append(
                PointStruct(
                    id=pid,
                    vector=vec,
                    payload={
                        "doc_id": doc.id,
                        "chunk_index": idx,
                        "content_markdown": chunk,
                        "metadata": meta,
                        "format": "markdown",
                    },
                )
            )
    if points:
        _client.upsert(collection_name=_collection, points=points, wait=True)
    return {"upserted": len(points)}


@app.post("/internal/query")
def query(req: QueryRequest) -> Dict[str, Any]:
    _ensure_collection()
    qvec = _embed_texts([req.query])[0]
    resp = _client.query_points(
        collection_name=_collection,
        query=qvec,
        limit=req.top_k,
        with_payload=True,
    )
    hits = resp.points
    docs: List[str] = []
    meta: List[Dict[str, Any]] = []
    for h in hits:
        pl = h.payload or {}
        text = str(pl.get("content_markdown") or pl.get("text", ""))
        docs.append(text)
        raw_meta = pl.get("metadata")
        chunk_meta = raw_meta if isinstance(raw_meta, dict) else {}
        meta.append(
            {
                "score": float(h.score),
                "doc_id": pl.get("doc_id"),
                "chunk_index": pl.get("chunk_index"),
                "format": pl.get("format", "markdown"),
                "metadata": chunk_meta,
            }
        )

    rerank_top = 3
    ce = _get_reranker()
    order: List[int] = list(range(len(docs)))
    if ce is not None and docs:
        pairs = [(req.query, d) for d in docs]
        scores = ce.predict(pairs)
        order = list(np.argsort(-np.array(scores)))
    top_idx = order[: min(rerank_top, len(order))]
    context = [
        {"text": docs[i], "meta": meta[i], "rerank_pos": int(pos)}
        for pos, i in enumerate(top_idx)
    ]
    return {"query": req.query, "context": context}
