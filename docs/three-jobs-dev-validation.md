# Three independent jobs — DEV validation, 2026-10-15

Target: `ivan_personal`; profile: `personal-env`; catalog: `workspace`.

The bundle now has exactly three jobs with no nested job tasks:

| Job | ID | Tasks |
| --- | --- | --- |
| Setup | `1056859271030616` | `create_schemas`, `create_inbox`, `seed_static_dimensions`, `prepare_gold_and_governance` |
| Events | `498267154097880` | `inbox_to_silver`, `build_gold_facts`, `check_gold` |
| Reference | `1088720123210134` | `batch_medallion`, `dim_project`, `check_reference` |

Setup is manual, with no schedule or pipeline tasks. It creates an empty fact
table only if absent and applies governance before first publication. Repeated
setup must not replace existing fact rows. The separate `check_dimensions` task
was removed; preflight executes inside `build_gold_facts`, before fact replacement.

The events job retains bundle resource key `wikimedia_medallion` and its existing
ID; the UI name is now `wikimedia_events_ivan_personal` (with the DEV prefix).
Both existing Lakeflow pipeline IDs were retained. Reference processing is
unchanged. The streaming pipeline still reads the single Bronze inbox directly.

## Executed verification

- [Setup run 1075407490247393](https://dbc-ab9a5afe-c151.cloud.databricks.com/jobs/1056859271030616/runs/1075407490247393): **SUCCESS**, all four tasks passed.
- Before and after setup, the fact table had **12,903 rows** and the same table
  ID (`33b1d1ca-14de-4518-b6c2-2e705ab14ba1`). The user mask and row filter were present.
- [Events run 806864773969845](https://dbc-ab9a5afe-c151.cloud.databricks.com/jobs/498267154097880/runs/806864773969845): **SUCCESS**, all three tasks passed.
- Final Silver/Gold counts: **12,903 / 12,903**; duplicate Silver keys: **0**.
- Static date rows: **1,095**; editor types: **2**.
- Gold column mask and row filter remained attached after fact replacement.
- Both processing schedules remain paused; setup has no schedule.

37 relevant local tests, Flake8, strict DEV bundle validation, and
`git diff --check` passed. The local Gold test checks both initial empty fact
creation and repeated setup after inserting facts. Workspace execution checks
actual Delta and Unity Catalog behavior. Reports are stored locally under the
ignored `.state/three-jobs-dev-validation/` directory.

See [job responsibilities](jobs.md) and [first installation](manual-setup.md).

Task names were subsequently clarified without changing their behavior or job
IDs. The run/task names above describe the executed validation runs; see
[job responsibilities](jobs.md) for current names. The naming-only deployment
passed five bundle tests, lint, strict validation, and a read-back comparison of
all deployed task keys/dependencies. No processing rerun was needed for renaming.
