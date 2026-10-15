# RAG DEV validation — 2026-10-15

This report records the initial one-article verification. For the subsequent
40-article release and English interface, see [rag-demo-40.md](rag-demo-40.md).

Profile `personal-env`, target `ivan_personal`, AWS `us-east-2`.
The user authorized deployment and a run scoped to one existing Silver event.
No new ingestion session, ten-article expansion, or documentation ingestion ran.
No account subscription, payment method, or cloud provider configuration changed.

## Input and preparation

Current SQL statement `01f1c834-d95c-18d6-83dd-cdf91be4d984` confirmed exactly one
eligible Silver event in `[2026-10-15T00:27:16Z,2026-10-15T00:27:17Z)`:
`Surinam Airways`, meta_id `2fc269f6-9f0a-4bc5-ad67-8440aa28e794`.
Its upstream session had already completed the medallion workflow.

[Preparation run 667021071465626](https://dbc-ab9a5afe-c151.cloud.databricks.com/jobs/638980765974917/runs/667021071465626?o=7474655630219513)
finished SUCCESS. It made exactly one Wikipedia HTTP request, saved one canonical
document (`enwiki:2073498`, revision `1373935683`) and one chunk.
The saved summary was fetched at `2026-10-15T01:09:04.007503Z`.

Source: [Surinam Airways](https://en.wikipedia.org/wiki/Surinam_Airways).
The saved extract describes the flag carrier of Suriname, based in Paramaribo,
with regional/long-haul services and government ownership.

Corpus `0fcba528f4cdcfa0411e3a7c`; generation `050cdc677e005c8afd0ecf6d`.

## Managed index and free compute limits

The index `workspace.wikimediademo_ai.wikimedia_summary_index` was created as a
TRIGGERED managed DELTA_SYNC index, embedding `text` with the existing managed
`databricks-qwen3-embedding-0-6b` API. Delta CDF is enabled on `knowledge_chunks`.

It reuses `ivanrazumovskyi-lab12-steam-dev`; no second Search endpoint was created.
The pre-existing `chunks_index` and `board_chunks_index` were not changed.
Index initialization update `0d183fe1-c38a-4020-8554-71b0634353b1` completed.

A subsequent sync held a serverless Job driver open and hit the workspace's free
serverless capacity limit. The managed pipeline reported:
`RESOURCE_EXHAUSTED: You've hit the limit for severless compute for free usage`.
The failed Job/retry was canceled and the SQL compute started for the test stopped.

The corrected path runs `scripts/sync_rag.py` locally so no notebook driver
competes with managed sync. New update
`a6acda20-8d89-49a9-bc78-3055f5a90d16` reached COMPLETED; index ready=true,
indexed_row_count=1. Only after completion did SQL publish the READY manifest.
This does not bypass quotas or upgrade the subscription. Managed retry handling
was also corrected to preserve the original update's failure instead of calling
its automatic retry an unrelated concurrent sync.

## Real retrieval, LLM and idempotency

[Smoke run 791216984408975](https://dbc-ab9a5afe-c151.cloud.databricks.com/jobs/638980765974917/runs/791216984408975?o=7474655630219513)
finished SUCCESS using managed retrieval and `databricks-gemma-3-12b`.

- The article question retrieved the expected Wikipedia URL.
- The answer identified Suriname's flag carrier, Paramaribo, service scope and
  government ownership; these claims match the saved extract.
- The workspace-password/future-count question was refused with no source.
- Repeated preparation made zero HTTP requests and retained exactly one chunk.
- Chunk rows stayed identical; no duplicate IDs, inserts, updates or deletes of
  chunk data occurred during the repeat. Delta history is checked only after
  the repeat's starting version, avoiding unrelated earlier commits.

A real five-case eval passed 5/5: topic, location, ownership, a password/future-count
question, and the reason for the historical edit. The final two were refused.
Questions and expected sources are in `tests/fixtures/rag_smoke_eval.json`.
This is limited evidence on one summary, not a broad quality benchmark.

## Databricks App

App `wikimedia-assistant-dev`, Medium compute, deployment
`01f1c83914a9100a91482868780494ec` reached SUCCEEDED and App RUNNING.
The API's 30-character app-name limit required shortening the original name;
a regression test now enforces this limit for both targets.

The App's OAuth service principal ran its own real retrieval and LLM question;
`APP_SP_SMOKE` in the App logs contains a supported answer and the correct URL.
Resource rights are SELECT on this index, parent catalog/schema use privileges,
and CAN_QUERY on Qwen/Gemma. No Silver read/write or SQL warehouse access is
attached to the App.

The authenticated Streamlit `/_stcore/health` endpoint returned HTTP 200 and `ok`.
Browser interactions/history/source buttons were not manually exercised.

App URL: https://wikimedia-assistant-dev-7474655630219513.aws.databricksapps.com

After the bounded smoke, the App compute and the warehouse used by the test were
stopped. The successful deployment remains available for restart:

```sh
databricks apps start wikimedia-assistant-dev -p personal-env
```

Current deployment includes the optional one-question startup access smoke.
Normal packaging defaults to disabling that check.

## Retained resources and checks

- Standalone Job `638980765974917` (`wikimedia_rag_ivan_personal`), no schedule.
- Four Delta tables in `workspace.wikimediademo_ai`: article_content,
  article_aliases, knowledge_chunks, corpus_manifest.
- One READY managed RAG index on the existing endpoint; no active sync.
- App deployment retained, compute stopped after verification.
- Existing medallion resources and unrelated indexes retained unchanged.
- 95 local tests passed; Flake8, compilation and `git diff --check` passed.
- Detailed run outputs, summary, eval, App logs and health evidence are in the
  ignored `.state/rag-smoke/` directory. No credentials were added to source.

The free-usage capacity error establishes a free resource quota, not a full
account billing audit. Published PAYG prices in `rag.md` are conditional reference
prices; they do not prove this account is being charged. No paid upgrade was made.
