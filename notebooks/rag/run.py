# Databricks notebook source
"""Standalone serverless Lakeflow Job; never called by medallion workflow."""
import json
from pathlib import Path
import sys

# Files are synced beside this notebook by the preparation bundle.
root = Path.cwd()
# Notebook current working directory is notebooks/rag; bundled absolute path is passed explicitly.
dbutils.widgets.text("repo_root", "")
root = Path(dbutils.widgets.get("repo_root"))
sys.path.insert(0, str(root / "src"))
sys.path.insert(0, str(root / "app/wikimedia_assistant"))

from databricks.sdk import WorkspaceClient
from wikimedia_rag.corpus import corpus_id
from wikimedia_rag.storage import Store, prepare
from wikimedia_rag.search import sync_and_wait
from wikimedia_rag.documents import prepare_documents
from retrieval import retrieve, answer

for name, default in {
    "action": "prepare", "base_corpus_id": "", "titles_json": "[]", "catalog": "workspace", "silver_schema": "wikimediademo_silver",
    "ai_schema": "wikimediademo_ai", "start": "", "end": "", "article_limit": "1",
    "max_attempts": "3", "contact": "", "refresh": "expired", "ttl_hours": "168",
    "http_timeout": "10", "http_delay": "1", "http_retries": "2",
    "index_name": "", "embedding_endpoint": "", "llm_endpoint": "", "sync_timeout": "1200",
}.items():
    dbutils.widgets.text(name, default)
p = {name: dbutils.widgets.get(name) for name in (
    "action", "base_corpus_id", "titles_json", "catalog", "silver_schema", "ai_schema", "start", "end", "article_limit",
    "max_attempts", "contact", "refresh", "ttl_hours", "http_timeout", "http_delay", "http_retries",
    "index_name", "embedding_endpoint", "llm_endpoint", "sync_timeout")}
limit = int(p["article_limit"])
titles = json.loads(p["titles_json"])
corpus = corpus_id(p["start"], p["end"], limit, titles) if p["action"] != "documents" else None
client = WorkspaceClient()


def preparation():
    return prepare(spark, catalog=p["catalog"], silver_schema=p["silver_schema"], ai_schema=p["ai_schema"],
                   start=p["start"], end=p["end"], article_limit=limit, max_attempts=int(p["max_attempts"]),
                   contact=p["contact"], refresh=p["refresh"], ttl_hours=float(p["ttl_hours"]),
                   timeout=float(p["http_timeout"]), delay=float(p["http_delay"]), retries=int(p["http_retries"]),
                   titles=titles)


if p["action"] == "prepare":
    report = preparation()
elif p["action"] == "documents":
    report = prepare_documents(spark, catalog=p["catalog"], ai_schema=p["ai_schema"],
                               root=root / "rag_docs", base_corpus_id=p["base_corpus_id"])
elif p["action"] == "sync":
    store = Store(spark, p["catalog"], p["ai_schema"])
    report = store.get_manifest(corpus)
    if report["status"] not in ("PREPARED", "READY"):
        raise RuntimeError("Only a successfully prepared corpus can be synced")
    store.manifest(report, "SYNCING")
    try:
        report["sync"] = sync_and_wait(client, p["index_name"], f"{store.prefix}.knowledge_chunks",
                                     p["embedding_endpoint"], timeout=int(p["sync_timeout"]))
        store.manifest(report, "READY")
    except Exception:
        store.manifest(report, "SYNC_FAILED")
        raise
elif p["action"] == "smoke":
    if limit != 1:
        raise ValueError("First smoke is strictly one article. Ten-article phase requires separate review.")
    store = Store(spark, p["catalog"], p["ai_schema"])
    report = store.get_manifest(corpus)
    if report["status"] != "READY":
        raise RuntimeError("Sync must finish before smoke")
    doc = report["documents"][0]
    question = f"What is {doc['canonical_title']} about?"
    rows = retrieve(client, p["index_name"], question, corpus, report["generation"])
    if not rows or doc["source_url"] not in {r["source_url"] for r in rows}:
        raise RuntimeError("Real retrieval did not return the expected source")
    grounded = answer(client, p["llm_endpoint"], question, rows)
    if not grounded["supported"] or doc["source_url"] not in {s["url"] for s in grounded["sources"]}:
        raise RuntimeError("LLM did not provide a grounded answer with the expected source")
    outside = "What is the secret password of this workspace and the exact number of edits tomorrow?"
    refusal = answer(client, p["llm_endpoint"], outside,
                     retrieve(client, p["index_name"], outside, corpus, report["generation"]))
    if refusal["supported"]:
        raise RuntimeError("Out-of-corpus question was not refused")
    from pyspark.sql import functions as F
    before = [r.asDict() for r in spark.table(f"{store.prefix}.knowledge_chunks")
              .filter(F.col("corpus_id") == corpus).orderBy("chunk_id").collect()]
    before_version = spark.sql(f"DESCRIBE HISTORY {store.prefix}.knowledge_chunks LIMIT 1").first().version
    # Verify cached idempotency with real Delta writes, without another HTTP call.
    p["refresh"] = "missing"
    repeated = preparation()
    after = [r.asDict() for r in spark.table(f"{store.prefix}.knowledge_chunks")
             .filter(F.col("corpus_id") == corpus).orderBy("chunk_id").collect()]
    if before != after or repeated["http_attempts"] != 0 or len({c["chunk_id"] for c in after}) != len(after):
        raise RuntimeError("Repeat changed chunks, called HTTP or introduced duplicates")
    history = (spark.sql(f"DESCRIBE HISTORY {store.prefix}.knowledge_chunks")
               .filter(F.col("version") > before_version).collect())
    for operation in history:
        metrics = operation.operationMetrics or {}
        if any(int(metrics.get(key, 0)) for key in ("numTargetRowsInserted", "numTargetRowsUpdated", "numDeletedRows")):
            raise RuntimeError("Unchanged repeat rewrote chunk data")
    report["smoke"] = dict(question=question, expected_source=doc["source_url"], grounded=grounded,
                           outside_question=outside, refusal=refusal, repeat_http_attempts=0,
                           chunks=len(after), eval_review="Human factual/source review still required")
    store.manifest(report, "READY")
else:
    raise ValueError("action must be prepare, documents, sync or smoke")

dbutils.notebook.exit(json.dumps(report, ensure_ascii=False))
