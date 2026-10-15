# Wikimedia ingestion with Zerobus

The personal DEV direct-inbox cutover has been validated end to end; see
[the current execution report](inbox-dev-validation.md). The earlier
[Zerobus integration report](zerobus-dev-validation.md) documents the original
migration from Event Hubs, before the redundant Bronze copy was removed.

Zerobus is the active ingestion path, replacing Event Hubs. It uses the same
SDK version (1.10.0), pinned workspace endpoints, OAuth M2M authentication and
explicit flush acknowledgment as `../databricks-dev-lab/lab11`.

```text
Wikimedia recentchange SSE
  → producer/zerobus/producer.py
  → Zerobus Ingest
  → <catalog>.<bronze_schema>.<raw_events_table> (the sole event Bronze table)
  → Lakeflow: Silver edits + quarantine
  → existing Gold workflow
```

The inbox is the append-only Bronze table created by the setup job.
Zerobus writes it; Lakeflow reads it directly and owns the downstream Silver
outputs. The Bronze adapter is a projection in a temporary view, not another
stored table. The batch reference pipeline runs in the independent
`wikimedia_reference` job.

The inbox contract is `event_json STRING, event_key STRING, produced_at BIGINT`.
`produced_at` is Unix microseconds. The full Wikimedia payload, including
`meta.id` and `_producer` metadata, is preserved in `event_json`. The in-memory Bronze adapter records
the source as `zerobus_wikimedia_recentchange`; Kafka audit columns are typed
nulls. The inbox's `produced_at` is the producer time, while the adapter's
`_ingested_at` is the downstream processing time.

## Credentials and workspace selection

Install from the project root with Python 3.12:

```bash
python -m venv .venv
.venv/bin/pip install -r tests/requirements-test.txt flake8
```

Local credential files are copied from Lab 11 and excluded from Git:

| Producer/bundle target | Local OAuth file | Catalog |
| --- | --- | --- |
| `ivan_personal` | `producer/zerobus/.env.personal` | `workspace` |
| `azure_trial_2` | `producer/zerobus/.env.azure_trial_2` | `dbr_dev` |

Each file contains `ZEROBUS_CLIENT_ID` and `ZEROBUS_CLIENT_SECRET`. With
`--env-file`, the producer ignores inherited OAuth credentials. Without the
flag, it reads those two environment variables from the process environment.
Local `.env.*` files and `.state/` replay manifests are Git-ignored;
`producer/zerobus/.env.example` contains empty placeholders only.

Hosts, endpoints and catalogs are pinned per target in
`producer/zerobus/targets.json`. That file contains no credentials. PROD also pins
its default Bronze schema to `wikimedia_prod_bronze` and table to `raw_events`;
DEV retains `wikimediademo_bronze.zerobus_wikimedia_inbox`. Change configuration
deliberately if a workspace changes. The producer cannot override
the catalog independently of its target. It accepts `--bronze-schema` so its
destination can match the bundle's schema variable.

The visibility check uses the selected target's Databricks CLI profile and SQL
warehouse from `targets.json`. That profile must resolve to the same workspace
and have permission to read the inbox and use the warehouse. Override the
warehouse with `--warehouse-id` or `BUNDLE_VAR_warehouse_id` when needed. OAuth
credentials in the env file are used for Zerobus ingestion; SQL reads use the
configured CLI profile.

## First DEV run

The existing deployment and `wikimediademo_*` schemas are reused. New
installations have only inbox → Silver. Before updating an existing deployment
with `bronze_stream`, follow [the inbox cutover procedure](inbox-cutover.md) to
copy legacy history and rebuild only Silver/quarantine from the complete inbox.
Do not simply change the Delta source on an existing checkpoint or refresh an
incomplete inbox: historical Event Hubs records may otherwise be lost.

```bash
export BUNDLE_VAR_warehouse_id=<personal-sql-warehouse-id>
export BUNDLE_VAR_zerobus_service_principal=<personal-oauth-application-id>

databricks bundle validate -t ivan_personal --strict
databricks bundle deploy -t ivan_personal
databricks bundle run -t ivan_personal wikimedia_setup
```

The setup job creates the inbox and grants the configured service principal
`USE CATALOG`, `USE SCHEMA`, and `SELECT, MODIFY` on the inbox. The deployment
identity must be able to grant these permissions. If permissions are managed
externally, leave `zerobus_service_principal` empty and grant access separately.
Use the application ID corresponding to the chosen OAuth file, not a secret.

Capture and send 100 events plus 20 intentional duplicate deliveries:

```bash
.venv/bin/python producer/zerobus/producer.py \
  --target ivan_personal \
  --env-file producer/zerobus/.env.personal \
  --count 100 --duplicates 20 \
  --manifest .state/zerobus-personal-001.json &&
# First install: run wikimedia_setup and wikimedia_reference first.
databricks bundle run -t ivan_personal wikimedia_medallion
```

For subsequent captures, use a new manifest and chain a successful producer run
to `databricks bundle run -t ivan_personal wikimedia_medallion`; the setup job
is only needed on first install or after setup/static-data/policy changes, as
described in [setup instructions](manual-setup.md).

Keep all bundle variable values the same for validate, deploy and run. The
event job runs the existing checks for Gold integrity and Silver/Gold counts.
Run `wikimedia_reference` separately to refresh SiteMatrix and `dim_project`;
events use its last successful publication.
The producer is external and must run before a triggered pipeline update if
that update is expected to process its events. It is a bounded capture command,
not an always-running daemon. `--timeout` defaults to 120 seconds and bounds
connection setup and SSE capture, including heartbeat-only connections. Capture
runs in a child process that is terminated at the deadline; process cleanup is
bounded separately. The Zerobus producer retains only events with `type = 'edit'`;
canaries and other change types are discarded before writing the manifest or
sending to Bronze. `--count` counts captured edits. Replay rejects manifests
containing non-edit events; capture a new manifest instead. Existing table data
is unchanged. Missing source IDs and malformed source events
are skipped with a warning. Silver DQ still handles invalid analytics fields.

## Replay and failure handling

```bash
.venv/bin/python producer/zerobus/producer.py \
  --target ivan_personal \
  --env-file producer/zerobus/.env.personal \
  --replay --manifest .state/zerobus-personal-001.json
```

Capture writes a private manifest before ingestion and refuses to overwrite it.
Replay preserves the exact payload and source IDs, and rejects a different
target or table. Do not edit manifests. They contain source event data, including
public usernames, and should be stored according to your data retention needs.

The SDK handles transient recovery. After `flush()` and a successful close,
the CLI polls Delta until every unique `event_key` from the manifest is queryable
in the inbox. Successful output includes `durability_confirmed: true` and
`queryable_confirmed: true`. `--visibility-timeout` defaults to 180 seconds;
the SQL query is submitted asynchronously and pending queries are cancelled on
timeout. Partial visibility and query failures exit with an error. The `&&`
above prevents starting the triggered workflow after such a failure.

A failed send or visibility check leaves the manifest available for replay.
Delivery is at least once: replay can
append duplicates to the Bronze inbox. Silver uses the existing source-ID
deduplication; this integration does not add a permanent historical uniqueness
guarantee or change the existing watermark policy. Flush confirms ingestion
durability, not queryability or completion of the downstream Lakeflow/Gold
workflow. The separate Delta check establishes queryability before starting
downstream processing; see [Zerobus concepts](https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-concepts).

`--dry-run` captures and saves a manifest without OAuth or ingestion. Combine it
with `--replay` to validate an existing manifest entirely offline.

For Azure, select `azure_trial_2` for **both** bundle and producer, use the Azure
OAuth file, Azure SQL warehouse and Azure principal ID. Do not reuse personal
credentials. No command automatically deploys or sends to Azure.

## Validation

```bash
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
.venv/bin/python -m pytest -q
.venv/bin/flake8 . --count --show-source --statistics
```

Local tests cover credential isolation, host/catalog selection, preserved source
IDs, manifest persistence and replay, ingestion/flush failures, setup permissions,
the Lakeflow source branch and the Bronze-to-Silver contract. Live OAuth,
permissions, Zerobus durability and Lakeflow execution require a workspace run.

References: [Zerobus setup and permissions](https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-ingest),
[Python SDK](https://github.com/databricks/zerobus-sdk-py).
