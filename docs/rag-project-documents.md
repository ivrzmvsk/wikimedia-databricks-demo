# Project documentation in the RAG collection

The `documents` action in the existing preparation job adds a snapshot of all
Markdown files under `rag_docs/` to a READY Wikipedia collection. The existing
index, embedding model, chat model and App are reused.

This release combines 40 Wikipedia summaries with 12 complete Markdown files.
Documentation contributes 84 chunks; the combined corpus has 124 chunks.

Markdown front matter supplies source titles. Chunking follows headings, repeats
the title/file/section on every chunk, and keeps short fenced SQL blocks with
their captions. Chunks are at most 1800 characters including the metadata prefix;
oversized paragraphs or code blocks are split at newline/word boundaries.

The combined corpus ID fingerprints the base corpus, its generation and sorted
file paths/content hashes. File changes or removals produce a new collection
snapshot. A repeated ingestion of the same files retains their loaded timestamps
and generation. The old Wikipedia-only collection is retained. No Wikipedia HTTP
requests are needed for this action.

The App shows separate Wikipedia and documentation counts. Wikipedia citations
open the article; documentation citations download the original Markdown file.
The App package contains only the indexed Markdown snapshots and checks their
hashes, so editing a file after ingestion requires preparation and sync again.
Documents are not published to GitHub. Source identifiers such as
`projectdoc:///rag_docs/01_architecture.md` are internal download references.

## Questions to try

- What is the Wikimedia Databricks Lakehouse Demo for?
- Why is there only one persisted Bronze event table, and how does Silver read it?
- В каком порядке запускать wikimedia_setup, wikimedia_reference и events при первой установке?
- Почему я вижу Jo*** вместо полного имени пользователя и какие группы видят исходные имена?
- Какие пять правил качества проверяют Wikimedia events в Silver?
- When does wikimedia_reference_freshness fire, and does it block event processing?
- Show the documented SQL recipe for bot share by language. Do not execute it.
- What is a corpus generation fingerprint and why does the App filter by both corpus ID and generation?

The assistant explains documented SQL; it does not execute it. Documentation is a
snapshot, so configuration defaults and historical details in the files are not
proof of current workspace settings, account billing or live table counts.

## Updating the documents

Deploy the preparation bundle after changing the Markdown files. Run its existing
job with `action=documents` and `base_corpus_id` pointing at the Wikipedia-only
READY corpus (currently `2448ad308f38b0bbb0f000a2`). Export the notebook result to a
local report, then use the existing sequential sync and App packaging path:

```sh
.venv/bin/python scripts/sync_rag.py .state/rag-project-docs/prepared.json --warehouse-id 6fec7d20a958edd1
.venv/bin/python scripts/package_rag_app.py .state/rag-project-docs/ready.json --verify-on-start
(cd rag/application && databricks bundle deploy -t ivan_personal)
(cd rag/application && databricks bundle run -t ivan_personal wikimedia_assistant --no-wait)
```

The existing job ID is `638980765974917`. No new job, serving endpoint or schedule
is required. The source folder is bounded to 50 `.md` files, 256 KB per file and
2 MB total; symlinks and ambiguous source paths are rejected.

## Validation evidence

- Local regression suite: 143 tests passed; flake8 and compilation passed.
- Preparation run `11770817048661` completed SUCCESS with zero HTTP requests.
- Combined corpus: `9fc2a8508a795d50a174e852`.
- Generation: `65a172de6c4d2127724252dd`.
- Reports are saved locally under `.state/rag-project-docs/`.
- Managed sync update `f52240fa-7bbc-481a-bf96-bcc789f8b2dd` completed.
- SQL verification confirmed 84 documentation chunks / 12 documents and
  40 Wikipedia chunks / 40 articles, with one generation in each group.
- The existing index contains 175 rows across historical and current collections;
  the App filters to the new combined corpus and generation.

Documentation retrieval now uses HYBRID search on the same index: semantic search
is combined with keyword matching for identifiers such as
`wikimedia_reference_freshness` and for SQL recipes. No index recreation or new
endpoint is needed. Mixed Wikipedia/documentation contexts still refuse requests
for the motive of a specific Wikipedia edit.

The real-service eval consists of the 30 article cases in `rag/demo_eval.json`
and 15 documentation cases in `rag/docs_eval.json`. For the shared data-flow
question, either the Sources or Architecture document is an acceptable citation;
both explicitly contain the same flow. The general dimension-purpose question also accepts the Overview or Gold model
document, which both define those dimensions. Other cases retain specific expected
sources. There is also a distinct producer delivery-semantics case.

Manual review caught an unsupported extra inference about inbox polling proving
Silver completion. The final instruction requests concise answers and explicitly
distinguishes inbox durability/queryability from downstream processing; the
rechecked delivery answer correctly states that flush does not confirm that the
pipeline ran. This remains a small demo evaluation, not an accuracy guarantee.

The final local regression suite passed **143 tests**.

Response language is explicitly set per question to keep English and Russian
answers stable alongside the English interface.

Final real-service evaluation: **45/45 passed** with the published configuration.
The report is `.state/rag-project-docs/eval-published.json`; it covers expected
citations, supported/refused outcomes, response language, original film titles,
alert thresholds and documented SQL. Critical delivery/alert/SQL answers were
also manually reviewed against the source Markdown.

Final deployment `01f1c8765e7818f880a0a7105bd95231` reached SUCCEEDED. The App's
OAuth service principal retrieved both Wikipedia and project-document sources.
`APP_DOCS_SMOKE` confirmed all twelve packaged files matched their indexed
content hashes. The authenticated Streamlit health endpoint returned HTTP 200.
The App was stopped after verification at the user's request; the deployed
collection remains ready for the next start. Final state evidence is
`.state/rag-project-docs/app-stopped.json`.

```sh
databricks apps start wikimedia-assistant-dev -p personal-env
databricks apps stop wikimedia-assistant-dev -p personal-env
```
