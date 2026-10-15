# Ivan Stream Week 7

This adds the Week-7 streaming part of the Wikimedia medallion project without
modifying the legacy Week-3 Structured Streaming notebooks or tables.

## Scope

Ivan owns the real-time Wikimedia stream:

- Lakeflow pipeline in Python using the current `pyspark.pipelines` API.
- Bronze stream ingestion from Azure Event Hubs.
- Parsed Silver table with data-quality rules.
- Quarantine table for bad stream records.
- Content-based deduplication.
- SQL alert for quarantined records.
- Pytest + chispa-ready local transformation tests.

## Tables

The new pipeline creates these Unity Catalog tables using fully qualified
Lakeflow dataset names:

- `dbr_dev.wikimediademo_bronze.bronze_stream`
- `dbr_dev.wikimediademo_silver.silver_wikipedia_edits`
- `dbr_dev.wikimediademo_silver.silver_wikipedia_edits_quarantine`

The legacy tables stay untouched:

- `dbr_dev.wikimediademo_bronze.wikipedia_edits_stream`
- `dbr_dev.wikimediademo_silver.wikipedia_edits`

## Bronze

`bronze_stream` reads Azure Event Hubs through the Kafka-compatible endpoint and
stores the raw event JSON plus Kafka metadata:

- `event_json`
- `kafka_topic`
- `kafka_partition`
- `kafka_offset`
- `kafka_enqueued_at`
- `_source`
- `_ingested_at`
- `_load_date`

Bronze intentionally does not deduplicate. It records what arrived from the
stream.

## Silver

`silver_wikipedia_edits` parses `event_json` into stable analytics columns and
keeps `event_json` for schema drift debugging.

Controlled schema evolution is demonstrated with optional producer metadata.
The producer already emits:

```json
"_producer": {
  "source": "wikimedia-recentchange",
  "producer_timestamp_utc": "..."
}
```

The new Silver schema exposes this as:

- `producer_source`
- `producer_timestamp_utc`

Old payloads without `_producer` continue to parse with null producer columns.

## Data Quality

The clean Silver table requires:

- valid JSON: `_parse_error = false`
- `event_id IS NOT NULL`
- `event_time IS NOT NULL`
- non-empty `wiki` and `domain`
- non-negative `length_old` and `length_new` when present

The clean Silver table uses Lakeflow `expect_all_or_drop` with the same rules.
The quarantine table reads the same staging view and keeps rows where
`_dq_passed = false`.

Bad records go to `silver_wikipedia_edits_quarantine` with:

- `_dq_passed`
- `_dq_failure_reasons`
- `_quality_status = 'quarantined'`

## Deduplication

Deduplication is content-based, not Kafka-offset based.

The key is `_wikimedia_event_key`:

- use `meta.id` when present;
- otherwise use `sha2(wiki | event_id | event_timestamp_unix | event_json)`.

Silver applies:

```python
.withWatermark("event_time", "10 minutes").dropDuplicates(["_wikimedia_event_key"])
```

Kafka metadata remains available for audit and debugging, but it is not used as
the deduplication key.

## Alert

The bundle-managed SQL alert is defined in `resources/alerts.yml`.
The following SQL/JSON files and UI steps are retained as manual reference only.
The SQL alert query is in `alerts/dq_quarantine_rate_query.sql`.
The API template is in `alerts/dq_quarantine_rate_alert.template.json`.

Alert condition:

```text
quarantine_records > 0
```

Run it hourly. It fires when any stream records failed data-quality rules and
landed in quarantine during the last hour.

To create it through the Databricks UI:

1. Open `SQL Editor`.
2. Paste the query from `alerts/dq_quarantine_rate_query.sql`.
3. Select a SQL warehouse and run the query once.
4. Click `Create alert`.
5. Set condition `quarantine_records > 0`.
6. Set schedule to every hour.

To keep the whole alert as code, use the template JSON and replace `<catalog>`.
It stores the alert condition, hourly schedule, and query lines:

```bash
cp alerts/dq_quarantine_rate_alert.template.json alerts/dq_quarantine_rate_alert.json
```

## Local Tests

Install test dependencies:

```bash
pip install -r tests/requirements-test.txt
```

Run tests:

```bash
pytest
```

The tests do not require Databricks, Unity Catalog, Event Hubs, or `dbutils`.

Jan can add batch/reference tests in the same suite as separate files, for
example `tests/test_batch_reference_cleanup.py` or `tests/test_batch_scd2.py`.
The shared Spark fixture lives in `tests/conftest.py`; stream-specific sample
payload helpers live in `tests/stream_test_helpers.py` and do not need to be
used by batch tests.

## Asset Bundle

The complete bundle now manages streaming, batch, Gold orchestration, governance,
and SQL alerts. See [README](../README.md) for configuration and
[the migration guide](bundle-migration.md) before adopting existing resources.

Set `DATABRICKS_BUNDLE_VAR_warehouse_id` to an existing SQL warehouse ID, then:

```bash
databricks bundle validate -t ivan_personal --strict
databricks bundle deploy -t ivan_personal
databricks bundle run -t ivan_personal wikimedia_medallion
```

The default `ivan_personal` target uses profile `personal-env` and catalog `workspace`.
The `azure_trial_2` target uses profile `azure_trial_2` and catalog `dbr_dev`.
Both retain the `wikimediademo_*` schema names and use development mode. The workflow creates schemas, runs both pipelines,
rebuilds Gold, checks integrity, and applies governance. Schedules start paused.
`wikimedia.repo_root` is supplied by the bundle using `${workspace.file_path}`;
manual workspace source paths are no longer needed.

## Gold Coordination

The bundle Gold fact notebook reads the new `silver_wikipedia_edits` table and
the project dimension reads active SCD2 reference records. Both are parameterized
with the same catalog/schema variables as the pipeline resources.
