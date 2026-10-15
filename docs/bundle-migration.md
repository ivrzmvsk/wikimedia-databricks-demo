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

## Current PROD table isolation

The `azure_trial_2` PROD target uses `dbr_dev.wikimedia_prod_bronze`,
`wikimedia_prod_silver`, and `wikimedia_prod_gold`, with the raw event table
`raw_events`. It does not adopt or overwrite shared `wikimediademo_*` tables.
Old event JSON is read and copied into the new raw table; old reference history
remains available in its legacy table. See [PROD isolation](prod-isolation.md).

Do not bind pipelines owned by other users or change their table properties to
work around ownership errors. The target retains its own existing job/pipeline
IDs and creates new datasets in schemas owned by the deployment identity.

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
   `/Workspace/Users/<deploying-identity>/.bundle/wikimedia-databricks-demo/azure_trial_2`.
   Use the same identity and state path locally and in CI. The Shared-root
   deployment is legacy; verify the existing resource IDs are retained when moving state.
2. Verify the three existing jobs/two pipelines are retained and the target uses
   only the new `wikimedia_prod_*` schemas. Old shared tables are read-only sources.
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
