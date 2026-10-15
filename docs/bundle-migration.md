# Migrating existing Databricks resources to the bundle

The bundle code is ready for deployment; it does not automatically discover or
adopt manually created resources by name. Deployment and data ownership are
separate: `bundle deploy` uploads code and creates/updates resource definitions;
`bundle run wikimedia_medallion` starts processing.

## Start with the ivan_personal target

1. The `ivan_personal` target uses `personal-env` / `workspace`; `azure_trial_2` uses
   `azure_trial_2` / `dbr_dev`. Select the intended target and set `BUNDLE_VAR_warehouse_id`.
2. Run `databricks bundle validate -t ivan_personal --strict`.
3. Deploy with schedules paused; follow [manual setup](manual-setup.md) if needed.
   Run `wikimedia_reference`, then `wikimedia_medallion` independently.
4. Check both independent job runs and their quality checks; confirm setup governance on first install.
5. Confirm the selected profile/catalog and override schemas if names are already in use.

The bundle requires serverless Jobs/Lakeflow, an existing catalog, SQL warehouse,
and Zerobus OAuth credentials. Schemas and the inbox are created by the manual setup job. Azure resources and the external producer keep their existing lifecycle.

## Existing Event Hubs deployment

Keep the current pipeline IDs and schema names. The streaming pipeline now
reads the Zerobus inbox directly and no longer defines a `bronze_stream` table.
For deployments that already have that table, follow [inbox cutover](inbox-cutover.md)
before running the new source: archive old tables for rollback, backfill missing
historical raw payloads into inbox, then explicitly refresh only Silver/quarantine
and rebuild Gold. Retire the old Bronze table only after coverage checks pass.
Existing Azure deployments require their own migration; the DEV cutover does not
migrate Azure history automatically.

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
export BUNDLE_VAR_bronze_schema=wikimediademo_bundle_bronze
export BUNDLE_VAR_silver_schema=wikimediademo_bundle_silver
export BUNDLE_VAR_gold_schema=wikimediademo_bundle_gold
```

These variables also redirect the Gold notebooks and alert queries. Keep the
settings consistent in CI and all subsequent commands. The streaming pipeline
reads the Zerobus inbox. New schemas do not contain old Event Hubs history;
plan a separate historical load if it is needed.

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

1. Configure repository Actions secrets `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, and
   `DATABRICKS_SQL_WAREHOUSE_ID`, and create the GitHub `azure_trial_2` environment.
   Use a stable PAT owner. The Azure target now uses
   `/Workspace/Shared/.bundle/wikimedia-databricks-demo/azure_trial_2` with no
   deployment mode or username in the path. Moving from
   the previous user folder changes the bundle state path; bind existing resources
   before deployment if you intend to adopt them instead of creating a second set.
2. Bind compatible existing resources or choose unused schemas.
3. Validate and deploy `azure_trial_2` with schedules paused.
4. Complete manual setup as needed, then run `wikimedia_reference` and
   `wikimedia_medallion`; inspect their checks; setup applies governance before processing.
5. Switch consumers from legacy Silver names to `silver_wikipedia_edits` where
   needed. The bundle Gold fact already reads this table.
6. Keep schedules paused for these targets and run the workflow
   manually: `wikimedia_reference` for reference refresh, `wikimedia_medallion`
   for events. A future scheduled deployment needs a target configured for it.

Do not use `bundle destroy` to roll back a migration of adopted pipelines: it can
remove their managed datasets. Keep the previous commit/configuration and treat
resource rollback and data restoration as separate operations.

## Splitting an existing combined workflow

The `wikimedia_medallion` resource key is retained, so the existing job is updated
in place to process events only. The streaming and batch pipeline resource keys
are unchanged; no table ownership or checkpoint migration is required.
The bundle has three jobs: manual `wikimedia_setup`, independent
`wikimedia_reference`, and the events job (display name `wikimedia_events`,
retained resource key `wikimedia_medallion`). The previous `wikimedia_initialize`
wrapper is absent. No job starts another job.

Keep schedules paused while deploying and running setup. The setup job creates
an empty fact table only if absent and attaches governance without replacing
existing facts. Existing Silver outputs and both pipeline IDs are retained.
Run setup, then reference and events independently to verify the deployment.
See [setup instructions](manual-setup.md). API failures in reference refresh do
not prevent events from using the last published project dimension. Configure
failure email recipients and SQL alert subscriptions before enabling schedules.
