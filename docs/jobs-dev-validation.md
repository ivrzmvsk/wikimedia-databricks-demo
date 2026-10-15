# Two-job DEV validation — 2026-10-15

Target: `ivan_personal`; profile: `personal-env`; catalog: `workspace`.

The final bundle contains exactly two independent processing jobs:

- `wikimedia_reference` (`1088720123210134`): batch pipeline → project dimension → reference checks.
- `wikimedia_medallion` (`498267154097880`): streaming pipeline and dimension preflight → facts → Gold checks.

The `wikimedia_initialize` wrapper was removed from the deployment. Setup,
static dimensions, and governance remain manual notebooks; see
[manual setup](manual-setup.md). Both existing Lakeflow pipeline IDs were retained.
No full refresh was requested. Both job schedules remain paused.

## Executed runs

- [Reference run 344752543565716](https://dbc-ab9a5afe-c151.cloud.databricks.com/jobs/1088720123210134/runs/344752543565716): **SUCCESS**, all three tasks passed.
- [Events run 771868698802494](https://dbc-ab9a5afe-c151.cloud.databricks.com/jobs/498267154097880/runs/771868698802494): **SUCCESS**, all four tasks passed.

The runs were started independently. The events job completed while the
reference job was still running, using the previously published dimension.

## Post-run verification

| Check | Result |
| --- | ---: |
| Inbox rows | 2,126 |
| Bronze event rows | 3,140 |
| Silver event rows | 3,049 |
| Quarantine rows | 26 |
| Gold fact rows | 3,049 |
| Project dimension rows including fallback | 1,074 |
| Duplicate Silver event keys | 0 |
| Reference freshness alert SQL result | 0 (fresh) |

The fact table retained the `user` column mask and editor-type row filter.
Deployed task lists contain no nested job tasks or setup/governance tasks.

Strict DEV bundle validation, 23 relevant local tests, Flake8, and
`git diff --check` passed. During the earlier deployment, the SQL Alerts API
rejected empty notification settings; the default now supplies a one-hour
retrigger interval with no subscribers. Recipients still require configuration.
Detailed run and verification JSON is stored locally under the ignored
`.state/jobs-split-dev-validation/` directory.
