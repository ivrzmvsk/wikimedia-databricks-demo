---
title: Bronze and Silver Layers - Parsing, Data Quality, Deduplication, Quarantine, SCD2
topic: bronze, silver, data quality
keywords: silver_wikipedia_edits, quarantine, dq rules, expect_all_or_drop, deduplication, watermark, meta_id, dedup key, scd2, auto cdc, lakeflow pipeline
---

# Bronze and Silver Layers

## Streaming pipeline: `wikimedia_streaming_medallion`

Source file: `pipelines/streaming_medallion/silver_wikipedia_edits.py`.
Serverless, ADVANCED edition, `continuous: false`. Pipeline configuration keys:
`wikimedia.repo_root`, `wikimedia.catalog`, `wikimedia.bronze_schema`,
`wikimedia.silver_schema`.

Datasets it defines:

| Dataset | Kind | Description |
| --- | --- | --- |
| `silver_wikipedia_edits_staging` | temporary view | Reads the inbox as a stream, adapts, parses, adds dedup key and DQ flags |
| `silver_wikipedia_edits` | streaming table | Clean, parsed, deduplicated events; rows failing DQ are dropped |
| `silver_wikipedia_edits_quarantine` | streaming table | Rows with `_dq_passed = false`, plus `_quality_status = 'quarantined'` |

### Processing steps (in order)

1. `decode_zerobus_events`: selects `event_json` and adds typed-null Kafka
   columns (`kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_enqueued_at`),
   `_source = 'zerobus_wikimedia_recentchange'`, `_ingested_at`, `_load_date`.
2. `parse_bronze_to_silver`: parses JSON with `from_json` using
   `WIKIPEDIA_EVENT_SCHEMA` and a corrupt-record column. Sets `_parse_error`.
3. `add_dedup_key`: adds `_wikimedia_event_key`.
4. `compute_dq_flags`: adds `_dq_<rule>` booleans, `_dq_passed`, `_dq_failure_reasons`.
5. `silver_wikipedia_edits` applies `expect_all_or_drop(DQ_RULES)` and
   `deduplicate_stream_records`.
6. `silver_wikipedia_edits_quarantine` keeps rows where `_dq_passed = false`.

### Silver columns of `silver_wikipedia_edits`

Event: `event_id`, `event_type`, `namespace`, `title`, `title_url`, `comment`,
`event_timestamp_unix`, `event_time` (timestamp from the Unix time),
`event_time_utc` (from `meta.dt`), `user`, `bot`, `minor`, `patrolled`,
`server_name`, `wiki`, `meta_id`, `domain`, `length_old`, `length_new`.

Producer and lineage: `producer_source`, `producer_timestamp_utc`, `event_json`,
`kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_enqueued_at`, `_source`,
`bronze_ingested_at`, `bronze_load_date`.

Derived: `editor_type` (`bot` if `bot = true`, otherwise `human`),
`byte_change` (`length_new - length_old`), `abs_byte_change`,
`silver_ingested_at`, `silver_load_date`.

Quality and keys: `_parse_error`, `_wikimedia_event_key`, `_dq_valid_json`,
`_dq_valid_event_id`, `_dq_valid_event_time`, `_dq_valid_wiki_domain`,
`_dq_valid_length`, `_dq_passed`, `_dq_failure_reasons`.

## Data quality rules (events)

Defined in `src/wikimedia_stream/dq_rules.py`:

| Rule | Expression |
| --- | --- |
| `valid_json` | `_parse_error = false` |
| `valid_event_id` | `event_id IS NOT NULL` |
| `valid_event_time` | `event_time IS NOT NULL` |
| `valid_wiki_domain` | `wiki` and `domain` are non-null and not blank |
| `valid_length` | `length_old` and `length_new` are NULL or `>= 0` |

`_dq_passed` is true only if all rules pass. `_dq_failure_reasons` is an array
of the names of the failed rules. A bad record is therefore never silently
lost: it appears in `silver_wikipedia_edits_quarantine` with its reasons.

To investigate rejected events:

```sql
SELECT _dq_failure_reasons, COUNT(*)
FROM <catalog>.wikimediademo_silver.silver_wikipedia_edits_quarantine
GROUP BY 1 ORDER BY 2 DESC;
```

## Deduplication

`src/wikimedia_stream/dedup.py`:

- Key column: `_wikimedia_event_key`.
- Value: `meta_id` if present; otherwise a SHA-256 of
  `wiki | event_id | event_timestamp_unix | event_json`.
- Streaming dedup: `withWatermark("event_time", "10 minutes")` followed by
  `dropDuplicates(["_wikimedia_event_key"])`.
- Consequence: a duplicate delivered more than about 10 minutes (event time)
  after the first copy might not be recognized as a duplicate by the streaming
  state. Large historical backfills must account for the watermark, because
  late-arriving old records can be dropped.
- Replayed producer manifests and deliberate duplicates (`--duplicates`) are
  handled by this deduplication.

## Batch reference pipeline: `wikimedia_batch_medallion`

Files: `pipelines/batch_medallion/bronze_reference.py` and `silver_reference.py`.

| Dataset | Description |
| --- | --- |
| `ref_wikimedia_projects` (Bronze) | SiteMatrix snapshot with `expect_all` on `wiki_code IS NOT NULL` and `project_name IS NOT NULL` |
| `silver_ref_staging` | Temporary view with DQ flags |
| `silver_ref_quarantine` | Rows where `_dq_passed = false` |
| `silver_ref_clean_snapshot` | Temporary view; `expect_all_or_drop(BATCH_DQ_RULES)` |
| `silver_ref_wikimedia_projects` | Streaming table filled by `create_auto_cdc_from_snapshot_flow` as SCD Type 2 |

Batch DQ rules (`src/wikimedia_batch/dq_rules.py`):
`valid_wiki_code` = `wiki_code IS NOT NULL`;
`valid_url` = `site_url LIKE 'http%' OR site_url IS NULL`.

SCD2 settings: key `wiki_code`; tracked columns `project_name`, `language_code`,
`site_url`; `stored_as_scd_type=2`. Lakeflow adds `__START_AT` and `__END_AT`.
Rows with `__END_AT IS NULL` are the currently active version of each project.

## FAQ

**Where do I find events that failed validation?**
In `silver_wikipedia_edits_quarantine`, column `_dq_failure_reasons`.

**How is a duplicate event identified?**
By `meta.id` (stored as `meta_id`); if missing, by a hash of wiki, event ID, timestamp and raw JSON.

**What does `__END_AT IS NULL` mean?**
The row is the current version of a project in the SCD2 history table.
