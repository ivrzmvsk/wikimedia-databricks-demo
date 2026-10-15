---
title: RAG Assistant - Corpus, Tables, Index, App and Cost Controls
topic: rag, assistant
keywords: rag, vector search, ai search, knowledge_chunks, article_content, corpus_manifest, wikipedia summary, delta sync index, databricks app, embedding, qwen, gemma, smoke test, citations
---

# RAG Assistant

## Purpose and isolation

The RAG assistant answers questions about Wikipedia articles that appeared in
the event stream. It reads `silver_wikipedia_edits` in a separate serverless
job and never changes the medallion path: no RAG dependencies exist in Silver,
and the events job does not depend on enrichment success.

## Three small bundles (release order)

| Bundle | Contents |
| --- | --- |
| `rag/preparation` | Standalone job `wikimedia_rag` with actions `prepare`, `sync`, `smoke` |
| `rag/serving` | Managed DELTA_SYNC vector index on an existing STANDARD AI Search endpoint; embeddings computed from `knowledge_chunks.text`; TRIGGERED sync |
| `rag/application` | Databricks App `wikimedia-assistant` and its resource permissions |

They use the direct deployment engine and require CLI 1.1.0 or newer. The
original bundle keeps its own engine and state. App creation and version
deployment are separate steps after a completed index sync.

## Corpus selection rules

Source module: `src/wikimedia_rag/corpus.py`.

- Explicit UTC interval `[start, end)`.
- Filter: `wiki = 'enwiki'`, `namespace = 0`, event type `edit` or `new`, non-empty title.
- Group by wiki and title; rank by event count descending, then title ascending.
- `article_limit` 1..10 counts usable canonical documents, not attempts.
- `max_attempts` bounds all HTTP requests, including retries and redirects.
- Optional explicit title list (must match `article_limit`).
- Corpus ID = first 24 hex characters of SHA-256 over normalized UTC bounds,
  limit, rules and optional titles.

Summaries come from the Wikipedia REST summary endpoint
`https://en.wikipedia.org/api/rest_v1/page/summary/<title>`. Requests are
sequential, at least 0.5 s apart (default 1 s), 10 s timeout, a descriptive
User-Agent `WikimediaAssistant/1.0 (<contact>)`, and a 429 response respects
`Retry-After`. Only `type = standard` pages with a non-empty extract and a valid
`en.wikipedia.org` URL are accepted. Redirect aliases are deduplicated by
canonical title and page ID. Document ID format: `enwiki:<pageid>`.

Document text is `title + description + extract`. Chunks are fixed
1800-character windows; chunk ID is a hash of corpus, document and position.

## Tables in `<catalog>.<ai_schema>` (default schema `wikimediademo_ai`)

| Table | Purpose |
| --- | --- |
| `article_content` | Summary cache: extract, description, URL, revision, fetched time, response JSON, text hash |
| `article_aliases` | Requested title to canonical document ID |
| `knowledge_chunks` | Text chunks and retrieval metadata; Delta Change Data Feed enabled |
| `corpus_manifest` | Selection interval and rules, candidate statuses, selected documents, generation, preparation/sync/smoke report |

Each corpus is a replacement snapshot of its interval and limit; other corpora
are preserved. Within a corpus, changed chunks are merged and obsolete chunk IDs
deleted; a repeated run with unchanged cache rewrites nothing.

**Generation** is a fingerprint of chunk IDs, content hashes, revisions, URLs
and fetch times. The App package pins both corpus ID and generation, and the
index search filters on both, so stale index rows or other corpora cannot enter
the App's context.

Refresh policies: `missing` (always reuse cache), `expired` (default, refetch
after `ttl_hours = 168`), `force` (refetch selected pages within the request budget).

## Index and models

- Index primary key `chunk_id`, index type DELTA_SYNC, pipeline type TRIGGERED,
  source table `knowledge_chunks`, embedding source column `text`.
- Default models: managed `databricks-qwen3-embedding-0-6b` for embeddings
  (multilingual questions against English summaries) and `databricks-gemma-3-12b`
  for chat. Both are pay-per-token endpoints; no dedicated serving endpoint is created.
- The embedding model is pinned per index. Never change the model or source
  schema in place; create a separately named new index, re-embed, then switch the App.
- The sync action checks source, primary key, mode, embedding column and model,
  triggers a sync, and waits for a **new** update to reach COMPLETED.

## Application package

The package script `scripts/package_rag_app.py` copies five application files
plus a sanitized `runtime.json`. It excludes contacts, raw summaries, raw events,
credentials, usernames, notebooks and repository documentation. It requires a
READY report with a completed sync update ID. App files: `app.py`, `app.yaml`,
`launch.py`, `requirements.txt`, `retrieval.py` (Streamlit-based UI).

The App answers only from retrieved context, cites source URLs, and refuses
questions outside the corpus (for example credentials or future event counts).

## Smoke test and evaluation

The `smoke` action requires `article_limit = 1`. It runs real retrieval, checks
the expected article URL, calls the LLM, validates source IDs, checks refusal
for an out-of-corpus question, reruns preparation from cache and verifies zero
HTTP requests, no duplicate keys and no inserted, updated or deleted chunk rows.
`rag/demo_corpus.json` defines a ten-article demo corpus (technology, space,
food, culture) and `rag/demo_eval.json` holds evaluation questions with expected
article titles. `scripts/evaluate_rag.py` runs them.

## Cost controls

- AI Search STANDARD endpoint has a minimum of 4 DBU/hour; billing for an
  endpoint starts with its first index and stops 24 hours after the last index
  is removed.
- A Databricks App of size Medium uses 0.5 DBU/hour while running; stop the App
  when it is not needed: `databricks apps stop wikimedia-assistant-dev`.
- Foundation model usage is billed per token (embedding 0.286 DBU per million
  input tokens; Gemma 3 12B 2.143 DBU per million input and 7.143 DBU per
  million output tokens).
- Cleanup destroys only the RAG application and serving bundles:
  `bundle destroy` in `rag/application` and `rag/serving`. Never destroy the
  original bundle or unrelated search endpoints. Delta tables and the
  preparation job can remain; they have no scheduled compute.

## FAQ

**Which articles are in the corpus?** Those selected by the interval rules or the explicit title list in `rag/demo_corpus.json`.
**How do I update the knowledge base?** Run `prepare` with a new interval, run `sync`, package the App with the new READY report, deploy and run the App.
**Why does the assistant refuse a question?** The answer is not in the retrieved context.
**Does the RAG job affect Silver or Gold?** No. It only reads `silver_wikipedia_edits`.
