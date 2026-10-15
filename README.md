# Wikimedia Databricks Lakehouse Demo

[![Wikimedia CI Pipeline](https://github.com/ivrzmvsk/wikimedia-databricks-demo/actions/workflows/ci.yml/badge.svg)](https://github.com/ivrzmvsk/wikimedia-databricks-demo/actions/workflows/ci.yml)

Wikimedia streaming and reference-data processing on Azure Databricks, Delta
Lake, and Unity Catalog. Databricks Asset Bundles define the deployable resources
in this repository.

## Architecture

```text
Wikimedia SSE → Python producer → Zerobus → Delta inbox
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

Zerobus is the only source configured in the deployed streaming pipeline.
See [the Zerobus guide](docs/zerobus.md) for setup, local OAuth files and
capture/replay commands. The original Event Hubs producer and manual notebooks
remain historical references and are not run by the bundle.

The producer runs externally with Python in a virtual environment. The bundle deploys the
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
| `azure_trial_2` | `azure_trial_2` | `dbr_dev` | unset (explicit presets) |

Set the required warehouse ID for the selected workspace:

```bash
export BUNDLE_VAR_warehouse_id=<sql-warehouse-id>
export BUNDLE_VAR_zerobus_service_principal=<oauth-application-id>

databricks bundle validate -t ivan_personal --strict
databricks bundle deploy -t ivan_personal
databricks bundle run -t ivan_personal wikimedia_medallion --only setup
# Run the Zerobus producer as described in docs/zerobus.md before processing.
databricks bundle run -t ivan_personal wikimedia_medallion
```

The profile is selected by the target; no `-p` option is needed. Both targets use
`wikimediademo_bronze`, `wikimediademo_silver`, and `wikimediademo_gold` in their
respective catalogs/workspaces. Override schema variables when sharing a
workspace with other deployments. The setup task creates the three schemas;
the catalog must already exist. Workspace files and state live under
`/Workspace/Users/<deploying-identity>/.bundle/wikimedia-databricks-demo/ivan_personal`
for the personal target, and
`/Workspace/Shared/.bundle/wikimedia-databricks-demo/azure_trial_2` for the Azure
team demo target. Source code is uploaded to the `files` subfolder.
The producer's OAuth principal must have access to the selected workspace inbox.

Override deployment settings with environment variables:

```bash
export BUNDLE_VAR_catalog=workspace
export BUNDLE_VAR_zerobus_service_principal=<oauth-application-id>
```

Alternatively, use `--var="catalog=workspace,bronze_schema=my_bronze"` on bundle
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
export BUNDLE_VAR_warehouse_id=<azure-sql-warehouse-id>
databricks bundle validate -t azure_trial_2
databricks bundle deploy -t azure_trial_2
databricks bundle run -t azure_trial_2 wikimedia_medallion
```

Use the same deployment identity and variables for subsequent commands so they
share bundle state. The personal target uses development mode. The Azure target
has no deployment mode and explicitly sets `trigger_pause_status: PAUSED`;
workflow and alert schedules also default to `PAUSED`.
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

Pull requests and pushes to `main` run local tests and lint. Same-repository pull
requests, pushes, and manual runs also validate the bundle. Successful pushes to
`main` and manual runs on `main` deploy `azure_trial_2`, using the GitHub
`azure_trial_2` environment for both validation and deployment. CI authenticates with
a PAT and creates the named CLI profile from the host secret; the token stays in
environment variables. Deployment does not start a processing run. Configure
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

To run manually, open Actions → Wikimedia CI/CD Pipeline → Run workflow and
select `main`. Other branches validate without deploying. The workflow checks the
PAT with `current-user me` before validating the bundle and deploying. Local tests
use Python 3.12, Java 17, and the pinned dependencies in `tests/requirements-test.txt`.

The Azure target deploys under
`/Workspace/Shared/.bundle/wikimedia-databricks-demo/azure_trial_2`, independent of
the PAT owner. The Azure target omits deployment mode so that its shared path
does not require a username and resource names have no automatic `[dev ...]` prefix.
The CLI warns about Shared being writable by all workspace users; this is
intentional for the demo, and `--strict` treats that warning as an error.
This shared location is intended for the team demo. Folder access
does not grant access to jobs, pipelines, or Unity Catalog tables; configure those
permissions separately. Moving an existing deployment from a user folder to
Shared changes the bundle state location and can create a second set of resources.
Bind compatible existing resources before deploying at the new path; see the
migration guide.

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
dependencies, target profiles/catalogs, Gold joins, fallback project keys, and failed
reconciliation. Local Gold tests execute the SQL transformations against Parquet;
Delta replacement behavior, Lakeflow CDC, secret/network access, and Unity Catalog
policies require a workspace smoke run.

References: [bundle resource configuration](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/resources),
[AUTO CDC FROM SNAPSHOT](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes-from-snapshot).
