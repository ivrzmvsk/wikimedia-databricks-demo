# Wikimedia Databricks Lakehouse Demo

[![Wikimedia CI Pipeline](https://github.com/ivrzmvsk/wikimedia-databricks-demo/actions/workflows/ci.yml/badge.svg)](https://github.com/ivrzmvsk/wikimedia-databricks-demo/actions/workflows/ci.yml)

Wikimedia streaming and reference-data processing on Azure Databricks, Delta
Lake, and Unity Catalog. Databricks Asset Bundles define the deployable resources
in this repository.

## Architecture

```text
Hourly events job:
  Wikimedia SSE → external producer → Zerobus → Bronze inbox
    → streaming Lakeflow: Silver + quarantine
    → Gold facts (join last published dimensions) → quality checks

Daily reference job:
  SiteMatrix API → batch Lakeflow: Bronze → Silver SCD2 → dim_project

Manual setup job:
  schemas → inbox + static dimensions → empty Gold fact if absent + governance
```

The streaming path has one persisted Bronze event table: `raw_events` in PROD
(`zerobus_wikimedia_inbox` in the existing DEV target). Lakeflow reads it directly and writes Silver plus
quarantine; there is no second `bronze_stream` copy. See
[the one-time cutover procedure](docs/inbox-cutover.md) before updating an older
deployment that still reads `bronze_stream`.

Zerobus is the only source configured in the deployed streaming pipeline.
See [the Zerobus guide](docs/zerobus.md) for setup, local OAuth files and
capture/replay commands. The original Event Hubs producer and manual notebooks
remain historical references and are not run by the bundle.

The producer runs externally with Python in a virtual environment. The bundle deploys the
Databricks side: two triggered serverless Lakeflow pipelines, three serverless
jobs, and three SQL alerts. It does not provision Event Hubs, Azure storage,
Key Vault, the producer's hosting, or a dashboard UI.

The RAG preparation job, AI Search index and Databricks App are separate bundles
under `rag/`, restricted to the personal `ivan_personal` target. They have no
`azure_trial_2` target and are excluded from the Azure CI/CD deployment.
See [the RAG guide](docs/rag.md).

## Repository

| Path | Purpose |
| --- | --- |
| `databricks.yml` | Bundle targets, variables, and source synchronization |
| `resources/pipelines.yml` | Streaming and batch Lakeflow pipelines |
| `resources/workflow.yml` | Manual setup and independent events/reference jobs |
| `resources/alerts.yml` | Quarantine and reference reconciliation alerts |
| `pipelines/` | Lakeflow dataset definitions |
| `src/` | Shared transformations, schemas, DQ, and deduplication |
| `notebooks/` | Parameterized setup, Gold, governance, analytics, and checks |
| `producer/` | Zerobus producer directory and historical Event Hubs producer |
| `producer/zerobus/` | Python Zerobus producer, workspace targets and dependencies |
| `tests/` | Local PySpark, bundle, and Gold transformation tests |
| `00-setup/` through `05_bronze_dashboard_analytics/` | Historical manual notebooks, excluded from bundle synchronization |
| `alerts/` | Historical SQL/JSON exports, superseded by `resources/alerts.yml` |

`resources/*.yml` is the deployment source of truth. The old manual
`pipeline_settings.json` files have been removed. Original notebooks are retained
as references; the workflow runs the parameterized versions in `notebooks/`.

## Prerequisites

- Databricks CLI **0.279.0 or newer**, with an authenticated workspace profile.
- A Unity Catalog catalog (`workspace` for `ivan_personal`, `dbr_dev` for `azure_trial_2`) and access to create schemas,
  materialized views, streaming tables, tables, and functions in that catalog.
- Serverless Jobs and Lakeflow compute enabled, plus outbound access to the
  Wikimedia SiteMatrix API. The external producer needs access to Wikimedia SSE
  and the workspace Zerobus endpoint.
- An existing SQL warehouse, with permission to use it for the bundle alerts.
- A workspace Zerobus endpoint and Databricks OAuth M2M service principal with
  access to the managed inbox table. Event Hubs credentials are not required.
- Account groups used by the governance functions. Defaults retain the existing
  `data_engineers`, `admins`, and `admin_group` names; override them as needed.

Credentials are read through Databricks profiles/environment variables and local
ignored env files. Do not put client secrets in bundle YAML or variable files.
The deployment identity must have permission to manage the bundle resources.

## Deploy and run

The targets reuse the existing CLI profiles:

| Target | Profile | Catalog | Mode |
| --- | --- | --- | --- |
| `ivan_personal` (default) | `personal-env` | `workspace` | development |
| `azure_trial_2` (PROD) | `azure_trial_2` | `dbr_dev` | unset (explicit presets) |

Set the required warehouse ID for the selected workspace:

```bash
export BUNDLE_VAR_warehouse_id=<sql-warehouse-id>
export BUNDLE_VAR_zerobus_service_principal=<oauth-application-id>

databricks bundle validate -t ivan_personal --strict
databricks bundle deploy -t ivan_personal
# First installation: prepare tables/static data and governance.
databricks bundle run -t ivan_personal wikimedia_setup
databricks bundle run -t ivan_personal wikimedia_reference
# Run the Zerobus producer as described in docs/zerobus.md, then process events.
databricks bundle run -t ivan_personal wikimedia_medallion
```

The profile is selected by the target; no `-p` option is needed. DEV uses
`wikimediademo_bronze`, `wikimediademo_silver`, and `wikimediademo_gold` in
`workspace`. PROD (`azure_trial_2`) uses `wikimedia_prod_bronze`,
`wikimedia_prod_silver`, and `wikimedia_prod_gold` in `dbr_dev`, with the raw event
table named `raw_events`. Existing shared `wikimediademo_*` tables in Azure are
legacy sources and are not modified by these jobs.

Setup creates schemas; the catalog must already exist. Code and bundle state
live under `/Workspace/Users/<deploying-identity>/.bundle/wikimedia-databricks-demo/<target>`.
Use a consistent deployment identity in CLI and CI to share the same state.
Source is uploaded to the `files` subfolder. The producer principal needs access
to the selected raw event table. See [PROD isolation and migration](docs/prod-isolation.md).

Override deployment settings with environment variables:

```bash
export BUNDLE_VAR_catalog=workspace
export BUNDLE_VAR_zerobus_service_principal=<oauth-application-id>
```

Alternatively, use `--var="catalog=workspace,bronze_schema=my_bronze"` on bundle
commands. Use the same settings for validate, deploy, and run. Notebook parameters
are derived from these deployment variables, keeping Gold aligned with the
Lakeflow tables.

The jobs have separate lifecycles. See [task responsibilities](docs/jobs.md) for
the exact tasks and their inputs/outputs:

| Job | Schedule when enabled | Work |
| --- | --- | --- |
| `wikimedia_reference` | Daily at 03:00 Europe/Warsaw | Batch Bronze/Silver reference, `dim_project`, publication checks |
| `wikimedia_medallion` (display: `wikimedia_events`) | Hourly | Inbox → Silver/quarantine → facts with inline preflight → Gold checks |
| `wikimedia_setup` | Manual only | Schemas, inbox, static dimensions, empty fact if absent, governance |

The setup job does not call either processing job. On first installation run
[setup](docs/manual-setup.md), then reference, then events. Setup attaches Gold
policies to an empty fact table before first publication and preserves existing
fact data on repeated runs. Keep processing schedules paused during maintenance.
The event job retains its bundle resource key/ID; its UI name is now
`wikimedia_events_<target>`. No separate `check_dimensions` task exists: preflight
runs at the start of fact construction.

Event processing reads the last successfully published `dim_project`; it never
calls the SiteMatrix API or waits for its refresh job. Missing/empty dimensions
fail fact preflight with setup/reference job instructions. Stale reference data raises a
separate SQL alert after 48 hours and does not block events. Reference publication
rejects empty or duplicate-key snapshots, and publishes the fallback project in
the same transaction as the dimension. A failed reference job leaves the last
published Gold dimension available. Existing Gold integrity and reconciliation
checks still raise on failure.

To refresh the reference explicitly:

```bash
databricks bundle run -t ivan_personal wikimedia_reference
```

Gold tables retain the full-rebuild behavior (`CREATE OR REPLACE TABLE`). The
fact table keeps its columns, so existing Unity Catalog masks and row filters
are retained by replacement; governance is applied by the setup job before first publication. Policy
changes require rerunning the setup governance task.
See [Databricks policy retention](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/filters-and-masks/manually-apply).
The date dimension retains its 2025–2027 range and must be extended before 2028.
Analytics examples in `notebooks/gold/06_dashboard_queries.py` are manual SELECTs;
they do not refresh stored dashboard data and are outside the processing jobs.

To refresh an individual pipeline after schemas exist:

```bash
databricks bundle run -t ivan_personal wikimedia_batch_medallion
databricks bundle run -t ivan_personal wikimedia_streaming_medallion
```

These individual commands do not run setup or refresh downstream Gold tables.
The pipelines use triggered execution, not continuous execution. Run the
external producer separately when live events are needed.

## Azure target and existing resources

Read [the migration guide](docs/bundle-migration.md) before deploying into a
workspace with existing tables or manually created pipelines.

```bash
export BUNDLE_VAR_warehouse_id=<azure-sql-warehouse-id>
databricks bundle validate -t azure_trial_2
databricks bundle deploy -t azure_trial_2
# First installation: run setup before reference/events.
databricks bundle run -t azure_trial_2 wikimedia_setup
databricks bundle run -t azure_trial_2 wikimedia_reference
databricks bundle run -t azure_trial_2 wikimedia_medallion
```

Use the same deployment identity and variables for subsequent commands so they
share bundle state. The personal target uses development mode. The Azure target
has no deployment mode and explicitly sets `trigger_pause_status: PAUSED`;
workflow and alert schedules also default to `PAUSED`.
Run the workflow manually with `bundle run`. `azure_trial_2` is the PROD environment; it retains explicit paused presets
rather than adding bundle production-mode behavior.

Events run hourly and references daily when schedules are enabled. The alerts
check quarantine count, missing active Silver reference records, and reference
freshness at minutes 15, 30, and 45 respectively. All schedules remain `PAUSED`
by default. Setup is manual and has no schedule.

Configure notification recipients through bundle variables (for example, in an
ignored `.databricks/bundle/<target>/variable-overrides.json`):

```json
{
  "job_email_notifications": {"on_failure": ["your-team@example.com"]},
  "alert_notifications": {
    "retrigger_seconds": 3600,
    "subscriptions": [{"user_email": "your-team@example.com"}]
  }
}
```

Use actual recipients before deployment; the default recipient lists are empty, so
notifications are not sent until configured. Alerts have a one-hour notification
retrigger interval; this keeps the API notification settings nonempty even
without subscribers. These settings apply to all three jobs and all three alerts.
No notification destinations or recipients are created automatically.

Batch SCD2 uses `AUTO CDC FROM SNAPSHOT` over the current validated reference
snapshot. It tracks project name, language, and URL changes and closes records
missing from the new snapshot. The quarantine table reflects rejected rows from
the current reference snapshot. Streaming Silver retains the event-time watermark
and content-key deduplication.

## CI/CD

Pull requests and pushes to `main` run local tests and lint. Same-repository pull
requests, pushes, and manual runs also validate the bundle. After tests and bundle
validation pass, pushes to `main` (including merged pull requests) automatically
deploy `azure_trial_2`. Direct pushes to `main` also deploy. Manual runs started
with `Run workflow` on `main` remain available, using the GitHub
`azure_trial_2` environment for both validation and deployment. CI authenticates with
a PAT and creates the named CLI profile from the host secret; the token stays in
environment variables. After deployment, CI runs `wikimedia_setup` and waits for
completion; a setup failure fails the workflow. Reference and event processing
remain separate runs. The deploy job's concurrency lock covers both deployment
and setup. Configure
these repository Actions secrets under Settings → Secrets and variables → Actions:

- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN` — a PAT for that workspace with deployment permissions.
- `DATABRICKS_SQL_WAREHOUSE_ID`

The workflow shows separate jobs for local tests, Databricks validation, and
deployment. Pull requests run validation; the deployment job is skipped entirely.
The PAT must belong to the workspace specified by `DATABRICKS_HOST`.

Create the GitHub environment `azure_trial_2`. If it overrides these secrets, use
the intended workspace and PAT owner. The `ARM_*` secrets are
no longer used by this workflow.

To deploy manually, open Actions → Wikimedia CI/CD Pipeline → Run workflow and
select `main`. Open pull requests and manual runs on other branches do not deploy.
The workflow checks the
PAT with `current-user me` before validating the bundle and deploying. Local tests
use Python 3.12, Java 17, and the pinned dependencies in `tests/requirements-test.txt`.

The PROD target deploys under the authenticated identity's private workspace
folder. CLI and CI must use the same deployment identity and state path. The
previous Shared-root deployment used shared table names; the isolated deployment
retains its three job IDs and two pipeline IDs while writing only new
`wikimedia_prod_*` tables. Alerts were recreated because their parent folders
cannot be changed through the Alerts API. Old paused alerts remain legacy
objects, not the active bundle's subscriptions.

Other `BUNDLE_VAR_*` settings can be supplied in the deploy job's
`env` block. Bind existing resources with the CI deployment identity before its
first deployment to the selected workspace. Fork pull requests run local tests
and lint; they skip Databricks authentication, validation, and deployment.

## Local checks

Python 3.12 and Java 17:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r tests/requirements-test.txt flake8
export PYSPARK_PYTHON="$(command -v python)"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
pytest -q
flake8 . --count --show-source --statistics
```

Tests cover parsing, DQ, deduplication, special SiteMatrix projects, bundle task
dependencies, target profiles/catalogs, Gold joins, fallback project keys, failed
reconciliation, independent job dependencies, and initialization preflight. Local Gold tests execute the SQL transformations against Parquet;
Delta replacement behavior, Lakeflow CDC, secret/network access, and Unity Catalog
policies require a workspace smoke run.

References: [bundle resource configuration](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/resources),
[AUTO CDC FROM SNAPSHOT](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes-from-snapshot).
