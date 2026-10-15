---
title: Data Sources - Wikimedia Events, SiteMatrix and Zerobus Producer
topic: data sources, ingestion
keywords: wikimedia recentchange, sse, event schema, namespace, event type, bot, minor, patrolled, sitematrix, zerobus, producer, inbox, replay
---

# Data Sources

## Source 1: Wikimedia recentchange stream (events)

Endpoint: `https://stream.wikimedia.org/v2/stream/recentchange` (Server-Sent Events).
Each event describes one change on any Wikimedia project: an edit, a new page,
a log action, or a categorization.

### Event fields used by the pipeline

Defined in `src/wikimedia_stream/schema.py` (`WIKIPEDIA_EVENT_SCHEMA`):

| JSON field | Type | Meaning |
| --- | --- | --- |
| `id` | long | Recent-change ID (not globally unique across wikis) |
| `type` | string | `edit`, `new`, `log`, `categorize`, `external` |
| `namespace` | long | Wikipedia namespace; `0` is the main article namespace |
| `title` | string | Page title |
| `title_url` | string | URL of the page |
| `comment` | string | Edit summary written by the editor |
| `timestamp` | long | Unix time of the change (seconds) |
| `user` | string | Username or IP address of the editor |
| `bot` | boolean | True if the edit was flagged as made by a bot |
| `minor` | boolean | True if marked as a minor edit |
| `patrolled` | boolean | True if reviewed by a patroller |
| `server_name` | string | Server host, e.g. `en.wikipedia.org` |
| `wiki` | string | Project database name, e.g. `enwiki` |
| `meta.id` | string | Unique event ID (used as the deduplication key) |
| `meta.domain` | string | Domain, e.g. `en.wikipedia.org`; `canary` is a test domain |
| `meta.dt` | string | ISO event time |
| `length.old` / `length.new` | long | Page size in bytes before and after the change |
| `_producer.source` | string | Added by the producer (`wikimedia-recentchange`) |
| `_producer.producer_timestamp_utc` | string | Added by the producer |

### Wikipedia concepts that explain the data

- **Namespace 0** is the article space. Other namespaces include Talk (1),
  User (2), Wikipedia/project pages (4), File (6), Template (10), Category (14).
- **byte_change** = `length.new - length.old`. Positive means content was added,
  negative means removed. `abs_byte_change` is the absolute size of the edit.
- **Bot edits** are automated; a bot flag is set by the Wikimedia software.
  Bot share is a common quality/activity metric.
- **Minor edits** are small changes such as typo fixes, flagged by the editor.
- **Wiki code examples**: `enwiki` English Wikipedia, `dewiki` German Wikipedia,
  `commonswiki` Wikimedia Commons, `wikidatawiki` Wikidata, `enwiktionary`
  English Wiktionary.

## Source 2: SiteMatrix API (reference)

Endpoint: `https://en.wikipedia.org/w/api.php?action=sitematrix&format=json`.
`src/wikimedia_batch/transforms.py::fetch_and_parse_sitematrix` flattens it into rows:

| Column | Source | Meaning |
| --- | --- | --- |
| `wiki_code` | `site.dbname` | Project identifier, e.g. `enwiki` |
| `project_name` | `site.sitename` | e.g. `Wikipedia` |
| `language_code` | language block `code` (or `special`) | e.g. `en` |
| `site_url` | `site.url` | Project URL |

`add_batch_metadata` adds `_ingest_timestamp` and `_batch_run_id`
(`lakeflow-batch-pipeline`). The request uses a descriptive `User-Agent`.

## The Zerobus producer

Script: `producer/zerobus/producer.py`. It is an external, bounded capture
command, not a daemon. It runs outside Databricks, in a Python virtual environment.

- Reads the SSE stream, discards `canary` events, skips events without
  `meta.id` or with malformed metadata (logged as warnings), and adds the
  `_producer` object.
- Writes a replay **manifest** file before sending (refuses to overwrite one).
- Sends records with the Zerobus Python SDK (version 1.10.0) using OAuth M2M
  (service principal `ZEROBUS_CLIENT_ID` and `ZEROBUS_CLIENT_SECRET`).
- Calls `flush()` for durability acknowledgment, closes the stream, then polls
  the inbox with SQL until every `event_key` is queryable.
- Success output includes `durability_confirmed: true` and `queryable_confirmed: true`.

### Producer options

| Option | Meaning |
| --- | --- |
| `--target` | `ivan_personal` or `azure_trial_2` (workspace, endpoint, catalog pinned in `targets.json`) |
| `--env-file` | Local OAuth file, e.g. `producer/zerobus/.env.personal` (Git-ignored) |
| `--count N` | Number of events to capture |
| `--duplicates N` | Number of intentional duplicate deliveries to send |
| `--manifest PATH` | Replay manifest path |
| `--replay` | Resend an existing manifest (same payloads and IDs) |
| `--dry-run` | Capture without OAuth or ingestion; with `--replay` validates a manifest offline |
| `--timeout` | Capture deadline in seconds, default 120 (also covers heartbeat-only SSE) |
| `--visibility-timeout` | Seconds to wait for rows to be queryable, default 180 |
| `--bronze-schema` | Match the bundle's `bronze_schema` variable |
| `--warehouse-id` | Warehouse used for the visibility check |

Example: capture 100 events plus 20 duplicates and then run the events job:

```bash
.venv/bin/python producer/zerobus/producer.py \
  --target ivan_personal --env-file producer/zerobus/.env.personal \
  --count 100 --duplicates 20 --manifest .state/zerobus-personal-001.json &&
databricks bundle run -t ivan_personal wikimedia_medallion
```

Use a new manifest file for every capture.

## The inbox table contract

`<catalog>.<bronze_schema>.zerobus_wikimedia_inbox`:

| Column | Type | Meaning |
| --- | --- | --- |
| `event_json` | STRING | The full Wikimedia payload as JSON, including `meta.id` and `_producer` |
| `event_key` | STRING | The event's `meta.id` |
| `produced_at` | BIGINT | Producer time in Unix microseconds |

The table is created by the setup job with `delta.appendOnly = true`.
The service principal used by the producer receives `USE CATALOG`,
`USE SCHEMA` and `SELECT, MODIFY` on the inbox when the bundle variable
`zerobus_service_principal` (an OAuth application ID) is set.

## Delivery semantics

Delivery is at least once. Replay can append duplicates to the inbox.
Silver deduplicates by event ID. Flush confirms ingestion durability, not that
the downstream pipeline has run; that is why the producer then polls the inbox
for queryability before the pipeline is triggered.

## Credentials

OAuth client ID and secret are stored only in local Git-ignored env files
(`producer/zerobus/.env.personal`, `.env.azure_trial_2`) or environment
variables. `producer/zerobus/.env.example` has empty placeholders. With
`--env-file`, inherited OAuth variables are ignored. Never put secrets in bundle
YAML or variable files.
