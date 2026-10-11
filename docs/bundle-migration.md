# Migrating existing Databricks resources to the bundle

The bundle code is ready for deployment; it does not automatically discover or
adopt manually created resources by name. Deployment and data ownership are
separate: `bundle deploy` uploads code and creates/updates resource definitions;
`bundle run wikimedia_medallion` starts processing.

## Start with the ivan_personal target

1. The `ivan_personal` target uses `personal-env` / `workspace`; `azure_trial_2` uses
   `azure_trial_2` / `dbr_dev`. Select the intended target and set `DATABRICKS_BUNDLE_VAR_warehouse_id`.
2. Run `databricks bundle validate -t ivan_personal --strict`.
3. Deploy and run `wikimedia_medallion` in `ivan_personal`.
4. Check both Lakeflow updates, Gold checks, and governance in the Jobs UI.
5. Confirm the selected profile/catalog and override schemas if names are already in use.

The bundle requires serverless Jobs/Lakeflow, an existing catalog, SQL warehouse,
and the Event Hubs secret. It creates schemas at workflow runtime. Azure resources
and the external producer keep their existing lifecycle.

## Choose how to handle existing Azure table ownership

If existing tables are already owned by manually created Lakeflow pipelines,
use the existing pipeline IDs to bind those pipelines into bundle state before
deploying. Use the same identity/profile and variables that CI will use:

```bash
databricks bundle deployment bind wikimedia_streaming_medallion <stream-pipeline-id> -t azure_trial_2
databricks bundle deployment bind wikimedia_batch_medallion <batch-pipeline-id> -t azure_trial_2
databricks bundle validate -t azure_trial_2 --strict
```

Review the bound pipeline configuration before deployment: the bundle uses
serverless compute, triggered mode, and the files under `pipelines/`. Binding
preserves the resource identity; it does not make incompatible data/state changes
safe. In particular, batch CDC now consumes snapshots rather than a stream of
materialized-view updates. An existing CDC target may require a separately
planned rebuild because its sequencing/system-column types can differ. Do not
force a full refresh of an existing SCD2 table without preserving the history you
need. The streaming pipeline keeps its existing deduplication behavior.

A table created by an old notebook is not an adoptable pipeline resource. If
those tables occupy the target names, use new schema names for the first bundle
run, validate the outputs, and plan the consumer cutover. For example:

```bash
export DATABRICKS_BUNDLE_VAR_bronze_schema=wikimediademo_bundle_bronze
export DATABRICKS_BUNDLE_VAR_silver_schema=wikimediademo_bundle_silver
export DATABRICKS_BUNDLE_VAR_gold_schema=wikimediademo_bundle_gold
```

These variables also redirect the Gold notebooks and alert queries. Keep the
settings consistent in CI and all subsequent commands. The new streaming pipeline
starts at the earliest events still retained by Event Hubs; that is not a backfill
of historical events that have expired from Event Hubs. Plan a separate historical
load if the new schemas must contain all old data.

Do not run the legacy streaming consumers and bundle pipelines against the same
target tables. Stop legacy schedules at cutover after validating the new flow.
This repository does not stop, drop, rename, or overwrite existing cloud resources
as part of the code migration.

## Optional adoption of existing jobs and alerts

If a matching workflow or SQL v2 alert already exists, bind its resource ID before
deploying rather than creating a second copy:

```bash
databricks bundle deployment bind wikimedia_medallion <job-id> -t azure_trial_2
databricks bundle deployment bind wikimedia_quarantine <alert-v2-id> -t azure_trial_2
databricks bundle deployment bind bronze_silver_reconciliation <alert-v2-id> -t azure_trial_2
```

Legacy alert exports in `alerts/` are reference artifacts. The new alerts use
`MAX` over the single aggregate result, so a quarantine query returning `0` does
not trigger merely because it returned one result row. Notification subscribers
must be configured explicitly if notifications are needed.

## Azure cutover

1. Configure the stable CI deployment identity and GitHub `azure_trial_2` environment secrets.
2. Bind compatible existing resources or choose unused schemas.
3. Validate and deploy `azure_trial_2` with schedules paused.
4. Run the complete `wikimedia_medallion` workflow and inspect its outputs.
5. Switch consumers from legacy Silver names to `silver_wikipedia_edits` where
   needed. The bundle Gold fact already reads this table.
6. Keep schedules paused for these development targets and run the workflow
   manually. A future scheduled deployment needs a target configured for it.

Do not use `bundle destroy` to roll back a migration of adopted pipelines: it can
remove their managed datasets. Keep the previous commit/configuration and treat
resource rollback and data restoration as separate operations.
