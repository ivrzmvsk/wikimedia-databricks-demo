# Setup job and first installation

`wikimedia_setup` is a manual, unscheduled job. It creates schemas, the inbox,
static dimensions, and the empty fact table if needed, and applies governance.
It does not start the reference/events jobs and does not replace existing facts.
See [all task responsibilities](jobs.md).

Use the same bundle target and variables for validation, deployment, and runs:

```bash
databricks bundle validate -t ivan_personal --strict
databricks bundle deploy -t ivan_personal
databricks bundle run -t ivan_personal wikimedia_setup
databricks bundle run -t ivan_personal wikimedia_reference
# Optionally capture events with the external Zerobus producer first.
databricks bundle run -t ivan_personal wikimedia_medallion
```

Run each command after the previous one succeeds. For Azure use
`-t azure_trial_2` with its catalog and credentials. Deployment itself does not
execute setup or processing. Catalogs and governance account groups must exist.

Job notebook parameters come from bundle variables: catalog, Bronze/Silver/Gold
schemas, optional `zerobus_service_principal`, and governance group names. The
service principal receives inbox access when supplied; this does not provision
a producer host or create account groups.

On first install Gold governance is attached while the fact table is empty,
before the first processing job publishes facts. On later runs setup leaves
fact rows intact and updates policies. Static dimensions are rebuilt, so pause
processing schedules during maintenance. The calendar currently ends in 2027;
extend it before 2028.

For targeted maintenance, use the setup job's `--only` selection, for example:

```bash
databricks bundle run -t ivan_personal wikimedia_setup --only prepare_gold_fact_and_policies
```

This requires existing schemas and prerequisite tables. An uninitialized target
must run the full setup job first. For schema/inbox updates use the appropriate
tasks (prefix a task key with `+` to include its prerequisites).

Silver tables remain owned and created by their Lakeflow pipelines. The project
dimension remains owned by the reference job; setup does not publish a placeholder
reference snapshot or fetch the API. Once initialized, events use the latest
successful reference publication even if a later reference refresh fails.
