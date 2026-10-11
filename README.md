# Wikimedia Databricks Lakehouse Demo

[![Wikimedia CI Pipeline](https://github.com/ivrzmvsk/wikimedia-databricks-demo/actions/workflows/ci.yml/badge.svg)](https://github.com/ivrzmvsk/wikimedia-databricks-demo/actions/workflows/ci.yml)

Wikimedia streaming and reference-data processing on Azure Databricks, Delta
Lake, and Unity Catalog. Databricks Asset Bundles define the deployable resources
in this repository.

## Architecture

```text
Wikimedia SSE → Python producer → Azure Event Hubs
                                      ↓
                            streaming Lakeflow pipeline
                            Bronze → Silver + quarantine
                                      ↓
SiteMatrix API → batch Lakeflow pipeline → SCD2 project reference
                                      ↓
                     Gold dimensions → edit fact table
                                      ↓
                      quality checks → governance → analytics queries
```

The producer remains an external Docker application. The bundle deploys the
Databricks side: two triggered serverless Lakeflow pipelines, one serverless
workflow, and two SQL alerts. It does not provision Event Hubs, Azure storage,
Key Vault, the producer's hosting, or a dashboard UI.

## Repository

| Path | Purpose |
| --- | --- |
| `databricks.yml` | Bundle targets, variables, and source synchronization |
| `resources/pipelines.yml` | Streaming and batch Lakeflow pipelines |
| `resources/workflow.yml` | End-to-end task dependencies |
| `resources/alerts.yml` | Quarantine and reference reconciliation alerts |
| `pipelines/` | Lakeflow dataset definitions |
| `src/` | Shared transformations, schemas, DQ, and deduplication |
| `notebooks/` | Parameterized setup, Gold, governance, analytics, and checks |
| `producer/` | External Wikimedia SSE → Event Hubs container |
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
  Wikimedia SiteMatrix API and the Event Hubs Kafka endpoint.
- An existing SQL warehouse, with permission to use it for the bundle alerts.
- Event Hubs and a Databricks secret containing its connection string. Defaults:
  namespace `evhpl24databricks02`, hub `ivanrazumovskyi_evh`, secret scope
  `wikimediademo-kv-scope`, key `eventhub-connection-string`.
- Account groups used by the governance functions. Defaults retain the existing
  `data_engineers`, `admins`, and `admin_group` names; override them as needed.

Credentials are read through Databricks profiles/environment variables and the
secret scope. Do not put connection strings in bundle YAML or variable files.
The deployment identity must have permission to manage the bundle resources.

## Deploy and run

The targets reuse the existing CLI profiles:

| Target | Profile | Catalog | Mode |
| --- | --- | --- | --- |
| `ivan_personal` (default) | `personal-env` | `workspace` | development |
| `azure_trial_2` | `azure_trial_2` | `dbr_dev` | development |

Set the required warehouse ID for the selected workspace:

```bash
export DATABRICKS_BUNDLE_VAR_warehouse_id=<sql-warehouse-id>

databricks bundle validate -t ivan_personal --strict
databricks bundle deploy -t ivan_personal
databricks bundle run -t ivan_personal wikimedia_medallion
```

The profile is selected by the target; no `-p` option is needed. Both targets use
`wikimediademo_bronze`, `wikimediademo_silver`, and `wikimediademo_gold` in their
respective catalogs/workspaces. Override schema variables when sharing a
workspace with other deployments. The setup task creates the three schemas;
the catalog must already exist. Workspace files and state live under
`/Workspace/Users/<deploying-identity>/.bundle/wikimedia-databricks-demo/<target>`.
Event Hubs and its secret must be accessible from the selected workspace.

Override deployment settings with environment variables:

```bash
export DATABRICKS_BUNDLE_VAR_catalog=workspace
export DATABRICKS_BUNDLE_VAR_eventhub_namespace=<namespace>
export DATABRICKS_BUNDLE_VAR_eventhub_name=<hub>
export DATABRICKS_BUNDLE_VAR_secret_scope=<scope>
export DATABRICKS_BUNDLE_VAR_eventhub_secret_name=<secret-key>
```

Alternatively, use `--var="catalog=workspace,eventhub_name=my-hub"` on bundle
commands. Use the same settings for validate, deploy, and run. Notebook parameters
are derived from these deployment variables, keeping Gold aligned with the
Lakeflow tables.

The workflow runs setup, then the two pipelines and independent dimensions.
The project dimension waits for the batch pipeline; the fact table waits for
both Silver inputs and all dimensions. Gold integrity and reconciliation checks
raise on failure. Governance and analytics run after those checks succeed.
Gold tables retain the original full-rebuild behavior (`CREATE OR REPLACE TABLE`).
An empty event stream is allowed; referential integrity and reconciliation still
run. The date dimension retains the original 2025–2027 range.

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
export DATABRICKS_BUNDLE_VAR_warehouse_id=<azure-sql-warehouse-id>
databricks bundle validate -t azure_trial_2 --strict
databricks bundle deploy -t azure_trial_2
databricks bundle run -t azure_trial_2 wikimedia_medallion
```

Use the same deployment identity and variables for subsequent commands so they
share bundle state. Both targets use development mode, which keeps schedules
paused. The Azure target also explicitly sets `trigger_pause_status: PAUSED`.
Run the workflow manually with `bundle run`. There is no production target in
this bundle.

The workflow has an hourly schedule, and the alerts have schedules at minutes 15
and 30, ready for a future target that enables scheduling. Alerts track quarantine
count in the last hour and missing active Silver reference records. They have no
notification subscribers configured; add subscriptions in `resources/alerts.yml`
when a destination is available.

Batch SCD2 uses `AUTO CDC FROM SNAPSHOT` over the current validated reference
snapshot. It tracks project name, language, and URL changes and closes records
missing from the new snapshot. The quarantine table reflects rejected rows from
the current reference snapshot. Streaming Silver retains the event-time watermark
and content-key deduplication.

## CI/CD

Pull requests and pushes to `main` run local tests and lint. Pushes to `main`
and manual runs on `main` additionally validate and deploy `azure_trial_2`, using
the GitHub `azure_trial_2` environment. CI creates the named CLI profile from the
host secret; credentials stay in environment variables. Deployment does not start
a processing run. Configure these GitHub secrets before enabling deployment:

- `DATABRICKS_HOST`
- `ARM_CLIENT_ID`, `ARM_CLIENT_SECRET`, `ARM_TENANT_ID`
- `DATABRICKS_SQL_WAREHOUSE_ID`

Other `DATABRICKS_BUNDLE_VAR_*` settings can be supplied in the deploy job's
`env` block. Bind existing resources with the CI deployment identity before its
first deployment to the selected workspace. Fork pull requests do not receive deployment credentials or
run the deploy job.

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
dependencies, target profiles/catalogs, Gold joins, fallback project keys, and failed
reconciliation. Local Gold tests execute the SQL transformations against Parquet;
Delta replacement behavior, Lakeflow CDC, secret/network access, and Unity Catalog
policies require a workspace smoke run.

References: [bundle resource configuration](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/resources),
[AUTO CDC FROM SNAPSHOT](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes-from-snapshot).
