---
title: Architecture and Data Flow
topic: architecture
keywords: architecture, data flow, medallion, zerobus, inbox, lakeflow, silver, gold, quarantine, reference, sitematrix
---

# Architecture and Data Flow

## End-to-end flow of events

1. An external Python producer (`producer/zerobus/producer.py`) reads the
   Wikimedia SSE stream `https://stream.wikimedia.org/v2/stream/recentchange`.
2. The producer sends each event to Databricks through Zerobus Ingest.
3. Zerobus writes into the Bronze Delta table
   `<catalog>.<bronze_schema>.zerobus_wikimedia_inbox` (the inbox).
4. The streaming Lakeflow pipeline `wikimedia_streaming_medallion` reads the
   inbox directly, parses JSON, computes data quality flags, deduplicates, and
   writes Silver tables:
   - `silver_wikipedia_edits` - clean, deduplicated events
   - `silver_wikipedia_edits_quarantine` - events that failed DQ rules
5. A Gold notebook rebuilds `fact_wikipedia_edits` by joining Silver events to
   the last published Gold dimensions.
6. A validation notebook (`check_gold.py`) fails the job if integrity checks fail.

## End-to-end flow of reference data

1. The batch Lakeflow pipeline `wikimedia_batch_medallion` calls the SiteMatrix
   API `https://en.wikipedia.org/w/api.php?action=sitematrix&format=json`.
2. Bronze table `ref_wikimedia_projects` receives the flattened snapshot.
3. Silver table `silver_ref_wikimedia_projects` stores it as SCD Type 2
   (keys: `wiki_code`; tracked columns: `project_name`, `language_code`,
   `site_url`). Rejected rows go to `silver_ref_quarantine`.
4. The notebook `02_dim_project.py` publishes `dim_project` in Gold.

## Key design decisions

- **One persisted Bronze event table.** The inbox is the only stored Bronze
  table for events. The earlier `bronze_stream` copy was removed. Silver reads
  the inbox through an in-memory adapter (`decode_zerobus_events`) that adds
  metadata columns; it does not write a second copy of raw JSON.
- **Independent jobs.** Events and reference data have separate jobs and
  schedules. Jobs never start each other.
- **Events never call the SiteMatrix API.** The event job uses the last
  successfully published `dim_project`. If the reference job fails, the previous
  dimension stays available. A separate alert warns if it is older than 48 hours.
- **Zerobus is the only configured source** in the deployed bundle. Event Hubs
  is historical.
- **Triggered, not continuous.** Both pipelines run with `continuous: false`.
  They process what is in the inbox when triggered.
- **Full rebuild of Gold.** Gold tables use `CREATE OR REPLACE TABLE`.
  Unity Catalog masks and row filters on the fact table survive replacement
  because the column names are kept.
- **Bundle does not provision** Event Hubs, Azure storage, Key Vault, the
  producer's hosting, or a dashboard UI.

## Schemas

Both targets use the same schema names inside their own catalog:

| Layer | Schema (default) |
| --- | --- |
| Bronze | `wikimediademo_bronze` |
| Silver | `wikimediademo_silver` |
| Gold | `wikimediademo_gold` |
| RAG | `wikimediademo_ai` |

Catalog is `workspace` for target `ivan_personal` and `dbr_dev` for target
`azure_trial_2`. Schema names can be overridden with bundle variables
`bronze_schema`, `silver_schema`, `gold_schema`.

## Table inventory

| Table | Layer | Created by |
| --- | --- | --- |
| `zerobus_wikimedia_inbox` | Bronze | setup job (`setup_inbox.py`), written by Zerobus |
| `ref_wikimedia_projects` | Bronze | batch pipeline |
| `silver_wikipedia_edits` | Silver | streaming pipeline |
| `silver_wikipedia_edits_quarantine` | Silver | streaming pipeline |
| `silver_ref_wikimedia_projects` | Silver (SCD2) | batch pipeline |
| `silver_ref_quarantine` | Silver | batch pipeline |
| `dim_date` | Gold | setup job |
| `dim_editor_type` | Gold | setup job |
| `dim_project` | Gold | reference job |
| `fact_wikipedia_edits` | Gold | created empty by setup, rebuilt by events job |

## FAQ

**Why is there no `bronze_stream` table?**
It was a redundant copy of the inbox. Lakeflow now reads `zerobus_wikimedia_inbox`
directly. Existing deployments must follow the one-time cutover in the operations runbook.

**Does the events job wait for the reference job?**
No. It reads the last published `dim_project`.

**Is processing continuous?**
No. Pipelines are triggered; jobs run on schedules (hourly events, daily reference) or manually.
