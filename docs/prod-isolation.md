# PROD (`azure_trial_2`): private code and independent tables

`azure_trial_2` is the PROD environment. It uses profile `azure_trial_2`, Azure
workspace `7405618361895520`, catalog `dbr_dev`, and warehouse `d8b8ff9942968cb8`.
The catalog name is retained; PROD tables have their own schema names:

| Layer | Schema | Event table / purpose |
| --- | --- | --- |
| Bronze | `dbr_dev.wikimedia_prod_bronze` | `raw_events`; also the independent project reference snapshot |
| Silver | `dbr_dev.wikimedia_prod_silver` | Clean events, event quarantine, reference SCD2 history and quarantine |
| Gold | `dbr_dev.wikimedia_prod_gold` | Fact table, dimensions, and governance functions |

Existing Azure `wikimediademo_*` tables are legacy shared datasets. They belong
to different owners/pipelines and are not renamed, overwritten, or adopted.
Table basenames within the new Silver/Gold schemas retain the existing contracts;
the raw event table is now `raw_events`.

## Workspace and bundle state

Code, resources, and state use the deployment identity's private folder:

```text
/Workspace/Users/<deploying-identity>/.bundle/wikimedia-databricks-demo/azure_trial_2
```

The checked parent ACL grants management to the deploying user and administrators,
not the general workspace users group. Use the same identity for CLI and CI.
Existing job and pipeline IDs were retained in bundle state. Verify IDs in
`bundle plan` before changing paths; do not deploy a plan creating duplicate jobs.

Alerts cannot change `parent_path` through the Alerts API. They were unbound from
the new state and recreated in the private resource folder. The old paused alerts
remain legacy objects under Shared; they are not used by the active bundle.

## Initial run and history

1. Run `wikimedia_setup`. It creates only the new schemas/tables, attaches Gold
   policies before data is published, and grants the configured producer access.
2. Copy exact raw JSON from the old inbox and `wikimediademo_bronze.bronze_stream`
   into the new `raw_events`. Use append-only inserts with an anti join on raw
   JSON to make the copy repeatable. Old tables are read-only sources.
3. Check coverage and file volume before initial streaming processing. The initial
   raw snapshot must fit the intended backfill/watermark strategy; old event-time
   records in later microbatches may otherwise be dropped.
4. Run `wikimedia_reference` to populate the new reference and project dimension.
5. Run `wikimedia_medallion` (UI name `wikimedia_events_azure_trial_2`). Verify all
   previous clean event keys, clean-source coverage, quarantine, Silver/Gold counts,
   absence of duplicate keys, and Gold policies.
6. Point consumers at the new Gold schema. Existing legacy consumers are unchanged.

Old reference SCD2 history remains in the legacy table; the new reference history
starts with the current API snapshot. Historical event JSON is copied unchanged;
transport/processing columns in rebuilt Silver reflect the new ingestion path.
Do not delete legacy shared tables as part of this migration.

## Producer destination

`producer/zerobus/targets.json` pins PROD defaults to
`dbr_dev.wikimedia_prod_bronze.raw_events`. DEV defaults remain unchanged.
`--bronze-schema` can override the target's schema but the per-target raw table
name is pinned. Existing running producer commands must finish/restart with the
new config; old manifests targeting legacy tables intentionally fail replay
validation against the new destination.

The non-secret producer application ID can be supplied through
`BUNDLE_VAR_zerobus_service_principal` or the ignored per-target variable overrides.
OAuth client secrets remain in the ignored credential files, never bundle config.
Processing and alert schedules remain paused until explicitly enabled.
