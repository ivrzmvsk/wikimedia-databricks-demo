---
title: Jobs, Tasks, Schedules and First Installation Order
topic: jobs, orchestration
keywords: wikimedia_setup, wikimedia_medallion, wikimedia_events, wikimedia_reference, job tasks, schedule, cron, first installation, pause_status, bundle run
---

# Jobs, Tasks and Schedules

Defined in `resources/workflow.yml`. There are three independent serverless jobs
and two Lakeflow pipelines. Jobs do not start each other. All notebook tasks
receive the same parameters from bundle variables (`catalog`, `bronze_schema`,
`silver_schema`, `gold_schema`, `zerobus_service_principal`,
`data_engineers_group`, `admins_group`, `row_admin_group`).

Each job has `max_concurrent_runs: 1` and queueing enabled.

## wikimedia_setup (manual, no schedule)

Display name `wikimedia_setup_<target>`. Run on first installation and when
static data, configuration or governance policies change.

| Task | Depends on | Responsibility |
| --- | --- | --- |
| `create_schemas` | - | Create Bronze/Silver/Gold schemas if absent (`notebooks/setup.py`) |
| `create_event_inbox` | create_schemas | Create `zerobus_wikimedia_inbox` and grant the producer principal (`setup_inbox.py`) |
| `refresh_static_dimensions` | create_schemas | Build `dim_date` and `dim_editor_type` (`setup_static_dimensions.py`) |
| `prepare_gold_fact_and_policies` | create_event_inbox, refresh_static_dimensions | Create the empty fact table if absent, create/update mask and row filter (`setup_gold.py`) |

Setup never fetches SiteMatrix, ingests events, creates Silver tables, or starts
the other jobs. The catalog and account groups must already exist. It never
replaces existing fact data. Because static dimensions are rebuilt, pause
processing schedules during maintenance.

## wikimedia_events (resource key `wikimedia_medallion`)

Display name `wikimedia_events_<target>`. The resource key stays
`wikimedia_medallion` to keep the existing job ID:
`databricks bundle run -t <target> wikimedia_medallion`.

Schedule: hourly, `0 0 * * * ?`, timezone Europe/Warsaw.

| Task | Depends on | Responsibility |
| --- | --- | --- |
| `process_events_to_silver` | - | Run pipeline `wikimedia_streaming_medallion` (incremental, `full_refresh: false`) |
| `refresh_gold_edit_facts` | process_events_to_silver | Preflight, then rebuild `fact_wikipedia_edits` |
| `validate_gold_edit_facts` | refresh_gold_edit_facts | Run `check_gold.py` |

The producer is external and must have delivered events to the inbox before
this job processes them.

## wikimedia_reference

Display name `wikimedia_reference_<target>`. Schedule: daily at 03:00
Europe/Warsaw (`0 0 3 * * ?`).

| Task | Depends on | Responsibility |
| --- | --- | --- |
| `refresh_project_reference` | - | Run pipeline `wikimedia_batch_medallion` |
| `refresh_gold_project_dimension` | refresh_project_reference | Publish `dim_project` (`02_dim_project.py`) |
| `validate_gold_project_dimension` | refresh_gold_project_dimension | Run `check_reference.py` |

If the SiteMatrix API fails, the previous `dim_project` remains and events keep working.

## Schedule state

The variable `schedule_pause_status` defaults to `PAUSED`. The personal target
uses `mode: development`, which keeps schedules paused. The `azure_trial_2`
target sets `presets.trigger_pause_status: PAUSED`. Set the variable to
`UNPAUSED` where scheduling is allowed to enable the hourly and daily runs.
Email notifications come from the variable `job_email_notifications` (empty by default).

## First installation order

```bash
export BUNDLE_VAR_warehouse_id=<sql-warehouse-id>
export BUNDLE_VAR_zerobus_service_principal=<oauth-application-id>
databricks bundle validate -t ivan_personal --strict
databricks bundle deploy -t ivan_personal
databricks bundle run -t ivan_personal wikimedia_setup
databricks bundle run -t ivan_personal wikimedia_reference
# run the external Zerobus producer
databricks bundle run -t ivan_personal wikimedia_medallion
```

Run each command only after the previous one succeeds. Deployment alone does
not execute any job. Use the same target and variables for validate, deploy and run.
Targeted maintenance: `databricks bundle run -t <target> wikimedia_setup --only prepare_gold_fact_and_policies`
(requires existing schemas; prefix a task key with `+` to include prerequisites).

Run a single pipeline (does not refresh downstream Gold):

```bash
databricks bundle run -t ivan_personal wikimedia_batch_medallion
databricks bundle run -t ivan_personal wikimedia_streaming_medallion
```

## FAQ

**Which job do I run first on a new workspace?** `wikimedia_setup`, then `wikimedia_reference`, then events.
**Why did the events job fail with "Gold dimensions are empty"?** Setup and/or reference have not run; run them first.
**How often do jobs run?** Events hourly, reference daily at 03:00 Warsaw time, setup only manually.
