---
title: Deployment with Asset Bundles, Targets, Variables and CI/CD
topic: deployment, ci/cd, bundle
keywords: databricks bundle, databricks.yml, targets, ivan_personal, azure_trial_2, variables, BUNDLE_VAR, github actions, ci.yml, flake8, pytest, validate, deploy, workflow_dispatch
---

# Deployment and CI/CD

## Prerequisites

- Databricks CLI 0.279.0 or newer (`databricks_cli_version: '>= 0.279.0'`).
- Authenticated CLI profile for the target workspace.
- An existing Unity Catalog catalog and permission to create schemas, tables,
  streaming tables, materialized views and functions in it.
- Serverless jobs and Lakeflow compute; outbound access to the Wikimedia SiteMatrix API.
- An existing SQL warehouse (for alerts).
- A Zerobus workspace endpoint and an OAuth M2M service principal for the producer.
- Account groups used by governance functions.

## Targets

| Target | CLI profile | Catalog | Mode / notes |
| --- | --- | --- | --- |
| `ivan_personal` (default) | `personal-env` | `workspace` | `mode: development`, bundle root under the user's workspace folder |
| `azure_trial_2` | `azure_trial_2` | `dbr_dev` | no mode; `trigger_pause_status: PAUSED`; root `/Workspace/Shared/.bundle/<bundle>/azure_trial_2` |

The profile is chosen by the target; `-p` is not needed. There is no production target.

## Bundle variables (databricks.yml)

| Variable | Default | Meaning |
| --- | --- | --- |
| `catalog` | `workspace` | Unity Catalog catalog |
| `bronze_schema` / `silver_schema` / `gold_schema` | `wikimediademo_bronze` / `_silver` / `_gold` | Schema names |
| `warehouse_id` | (required) | SQL warehouse for alerts |
| `zerobus_service_principal` | empty | OAuth application ID granted inbox access |
| `schedule_pause_status` | `PAUSED` | Schedule state |
| `job_email_notifications` | `{}` | Job email notifications |
| `alert_notifications` | `retrigger_seconds: 3600`, no subscriptions | Alert notification settings |
| `data_engineers_group` / `admins_group` / `row_admin_group` | `data_engineers` / `admins` / `admin_group` | Governance groups |

Set with environment variables (`export BUNDLE_VAR_catalog=workspace`) or
`--var="catalog=workspace,bronze_schema=my_bronze"`. Use identical values for
validate, deploy and run. Synced paths: `src`, `pipelines`, `notebooks`.

## Commands

```bash
databricks bundle validate -t ivan_personal --strict
databricks bundle deploy   -t ivan_personal
databricks bundle run      -t ivan_personal <job-or-pipeline-key>
```

Resource keys: jobs `wikimedia_setup`, `wikimedia_reference`, `wikimedia_medallion`;
pipelines `wikimedia_batch_medallion`, `wikimedia_streaming_medallion`.

For Azure use `-t azure_trial_2` together with the Azure warehouse ID, Azure
OAuth file and Azure principal. Do not reuse personal credentials.

## Deployed resources

Two triggered serverless Lakeflow pipelines, three serverless jobs, three SQL
alerts. Pipelines are `serverless: true`, `edition: ADVANCED`, `channel: CURRENT`,
`continuous: false`. They are named `<name>_<target>`.

## CI/CD (GitHub Actions, .github/workflows/ci.yml)

Workflow name: "Wikimedia CI/CD Pipeline". Triggers: push to `main`, pull
requests into `main`, manual `workflow_dispatch`.

1. **test_and_lint** (Python 3.12, JDK 17): install `tests/requirements-test.txt`
   and flake8; run flake8 with `--select=E9,F63,F7,F82` and the notebook builtins
   (`spark`, `dbutils`, `display`, `CATALOG`, `GOLD`, `SILVER`, `BRONZE`, ...);
   compile RAG sources; run `pytest tests/ -v`.
2. **validate_bundle** (needs test_and_lint; skipped for pull requests from
   forks): environment `azure_trial_2`; configures a PAT profile from secrets,
   checks authentication with `databricks current-user me`, runs
   `databricks bundle validate -t azure_trial_2`, and validates the separate
   `rag/preparation` and `rag/serving` bundles. Nothing is deployed.
3. **deploy** (needs validate_bundle): runs only for `workflow_dispatch` on
   `main`; executes `databricks bundle deploy -t azure_trial_2`. Uses a
   `concurrency` group `wikimedia-bundle-azure_trial_2` with no cancellation.

GitHub secrets: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_SQL_WAREHOUSE_ID`
(environment `azure_trial_2`). Authentication type is PAT. The workflow
permission is `contents: read`.

## Tests (tests/)

PySpark unit tests run locally with Java 17: schema, transforms, DQ rules,
dedup, batch reference, Gold notebooks, bundle configuration, job
initialization, reference freshness, Zerobus producer behavior and RAG
corpus/services. Run locally:

```bash
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
.venv/bin/python -m pytest -q
.venv/bin/flake8 . --count --show-source --statistics
```

## Migration into existing workspaces

Before deploying into a workspace with existing tables or manually created
pipelines, read `docs/bundle-migration.md`. Workspace files and state live under
the bundle root path of the target; source code is uploaded to the `files` subfolder.

## FAQ

**Does `bundle deploy` start jobs?** No. Deploy creates resources only; use `bundle run`.
**Why did validate fail in CI?** Check the Actions secrets (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_SQL_WAREHOUSE_ID`) and `BUNDLE_VAR_warehouse_id`.
**How do I deploy from CI?** Actions -> Run workflow on `main`.
