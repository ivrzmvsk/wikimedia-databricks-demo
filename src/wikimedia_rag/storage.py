"""Delta storage. Only the RAG schema is writable; Silver stays read-only."""
import json
from datetime import datetime, timezone

from wikimedia_rag.corpus import (RULES, cached_document, chunk_document, corpus_id,
                                  generation, identifier, interval, normalize_titles)
from wikimedia_rag.enrichment import BudgetExhausted, PageError, SummaryClient


CONTENT_SCHEMA = """document_id STRING, wiki STRING, page_id BIGINT, canonical_title STRING,
 description STRING, extract STRING, source_url STRING, revision STRING, fetched_at STRING,
 document_type STRING, text STRING, content_hash STRING, response_json STRING"""
CHUNK_SCHEMA = """chunk_id STRING, document_id STRING, corpus_id STRING, text STRING,
 content_hash STRING, source_url STRING, title STRING, document_type STRING,
 fetched_at STRING, revision STRING, start_utc STRING, end_utc STRING, generation STRING"""
MANIFEST_SCHEMA = "corpus_id STRING, status STRING, generation STRING, report_json STRING, updated_at STRING"
ALIAS_SCHEMA = "wiki STRING, title STRING, document_id STRING"


class Store:
    def __init__(self, spark, catalog, ai_schema):
        self.spark = spark
        self.prefix = f"{identifier(catalog)}.{identifier(ai_schema)}"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.prefix}")
        for name, schema in [("article_content", CONTENT_SCHEMA), ("knowledge_chunks", CHUNK_SCHEMA),
                             ("corpus_manifest", MANIFEST_SCHEMA), ("article_aliases", ALIAS_SCHEMA)]:
            spark.sql(f"CREATE TABLE IF NOT EXISTS {self.prefix}.{name} ({schema}) USING DELTA")
        spark.sql(f"ALTER TABLE {self.prefix}.knowledge_chunks SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

    def merge(self, name, rows, schema, keys):
        if not rows:
            return
        from delta.tables import DeltaTable
        source = self.spark.createDataFrame(rows, schema=schema)
        match = " AND ".join(f"t.{key}=s.{key}" for key in keys)
        changed = " OR ".join(f"NOT(t.{c} <=> s.{c})" for c in source.columns if c not in keys)
        (DeltaTable.forName(self.spark, f"{self.prefix}.{name}").alias("t")
         .merge(source.alias("s"), match)
         .whenMatchedUpdateAll(condition=changed).whenNotMatchedInsertAll().execute())

    def cache(self, title):
        return self.cache_many([title]).get(title)

    def cache_many(self, titles):
        from pyspark.sql import functions as F
        aliases = (self.spark.table(f"{self.prefix}.article_aliases")
                   .filter((F.col("wiki") == "enwiki") & F.col("title").isin(titles)))
        content = self.spark.table(f"{self.prefix}.article_content")
        found = aliases.select("title", "document_id").join(content, "document_id").collect()
        return {r.title: {k: v for k, v in r.asDict().items() if k != "title"} for r in found}

    def manifest(self, report, status):
        report["status"] = status
        self.merge("corpus_manifest", [dict(corpus_id=report["corpus_id"], status=status,
                   generation=report.get("generation", ""), report_json=json.dumps(report),
                   updated_at=datetime.now(timezone.utc).isoformat())], MANIFEST_SCHEMA, ["corpus_id"])

    def get_manifest(self, corpus):
        from pyspark.sql import functions as F
        rows = self.spark.table(f"{self.prefix}.corpus_manifest").filter(F.col("corpus_id") == corpus).collect()
        if not rows:
            raise RuntimeError("Corpus not prepared")
        return json.loads(rows[0].report_json)

    def reconcile_chunks(self, corpus, chunks):
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F
        self.merge("knowledge_chunks", chunks, CHUNK_SCHEMA, ["chunk_id"])
        ids = [c["chunk_id"] for c in chunks]
        # Session corpus is a replacement snapshot; other corpora are preserved.
        DeltaTable.forName(self.spark, f"{self.prefix}.knowledge_chunks").delete(
            (F.col("corpus_id") == corpus) & ~F.col("chunk_id").isin(ids))


def candidates(spark, catalog, silver_schema, start, end, cap, titles=None):
    from pyspark.sql import functions as F
    a, b = interval(start, end)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    source = spark.table(f"{identifier(catalog)}.{identifier(silver_schema)}.silver_wikipedia_edits")
    if titles:
        source = source.filter(F.col("title").isin(titles))
    return (source
            .filter((F.col("event_time") >= F.lit(a)) & (F.col("event_time") < F.lit(b))
                    & (F.col("wiki") == "enwiki") & (F.col("namespace") == 0)
                    & F.col("event_type").isin("edit", "new")
                    & (F.length(F.trim(F.col("title"))) > 0))
            .groupBy("wiki", "title").agg(F.count("*").alias("event_count"))
            .orderBy(F.desc("event_count"), F.asc("title")).limit(cap))


def prepare(spark, *, catalog, silver_schema, ai_schema, start, end, article_limit,
            max_attempts, contact, refresh="expired", ttl_hours=168, timeout=10, delay=1, retries=2,
            titles=None):
    titles = normalize_titles(titles, article_limit)
    corpus = corpus_id(start, end, article_limit, titles)
    if not article_limit <= max_attempts <= 100:
        raise ValueError("max_attempts must be >= article_limit and <=100")
    now = datetime.now(timezone.utc).isoformat()
    cached_document(None, now, ttl_hours, refresh)
    client = SummaryClient(contact, max_attempts=max_attempts, retries=retries, timeout=timeout, delay=delay)
    store = Store(spark, catalog, ai_schema)
    selected = candidates(spark, catalog, silver_schema, start, end, max_attempts, titles).collect()
    if titles and {r.title for r in selected} != set(titles):
        raise RuntimeError("Some explicit articles have no eligible Silver events in the UTC interval")
    report = dict(corpus_id=corpus, start=start, end=end, article_limit=article_limit,
                  rules=RULES, prepared_at=now, pages=[], http_attempts=0, documents=[],
                  refresh=refresh, ttl_hours=ttl_hours,
                  selection_mode="explicit_titles" if titles else "frequency", selected_titles=titles)
    store.manifest(report, "PREPARING")
    documents = {}
    cached = store.cache_many([r.title for r in selected])
    fetched, aliases = {}, []
    try:
        for row in selected:
            page = row.asDict()
            try:
                document = cached_document(cached.get(row.title), now, ttl_hours, refresh)
                page["cache_hit"] = document is not None
                if document is None:
                    document = client.fetch(row.title, now)
                    fetched[document["document_id"]] = document
                    aliases.append(dict(wiki="enwiki", title=row.title, document_id=document["document_id"]))
                page["document_id"] = document["document_id"]
                page["status"] = "duplicate_redirect" if document["document_id"] in documents else "usable"
                documents.setdefault(document["document_id"], document)
            except BudgetExhausted as exc:
                page.update(status="budget_exhausted", error=str(exc))
                report["pages"].append(page)
                break
            except PageError as exc:
                page.update(status="unusable", error=str(exc))
            report["pages"].append(page)
            if len(documents) == article_limit:
                break
        report["http_attempts"] = client.attempts
        # Batch Delta writes; keep usable responses even when the corpus is incomplete.
        store.merge("article_content", list(fetched.values()), CONTENT_SCHEMA, ["document_id"])
        store.merge("article_aliases", aliases, ALIAS_SCHEMA, ["wiki", "title"])
        if len(documents) != article_limit:
            raise RuntimeError(f"Only {len(documents)}/{article_limit} usable documents; corpus not published")
        chunks = [c for doc in documents.values() for c in chunk_document(doc, corpus, start, end)]
        version = generation(chunks)
        for c in chunks:
            c["generation"] = version
        report.update(generation=version, chunk_count=len(chunks),
                      documents=[{k: d[k] for k in ("document_id", "canonical_title", "source_url",
                                 "content_hash", "fetched_at", "revision")} for d in documents.values()])
        store.reconcile_chunks(corpus, chunks)
        store.manifest(report, "PREPARED")
        return report
    except Exception as exc:
        report.update(error=str(exc), http_attempts=client.attempts)
        store.manifest(report, "FAILED")
        raise
