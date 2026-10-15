# Direct inbox → Silver DEV validation — 2026-10-15

Target: `ivan_personal`; profile: `personal-env`; catalog: `workspace`.

The streaming pipeline now reads `wikimediademo_bronze.zerobus_wikimedia_inbox`
directly. Its only library is `silver_wikipedia_edits.py`; the former Bronze
append flow and `bronze_stream.py` definition were removed. Existing pipeline
ID `6eb6d32f-21af-44d1-8223-d15c2dc34733` and Silver output names were retained.
The two jobs and the batch reference pipeline were not reorganized further.

## Migration

- Confirmed both processing jobs and both pipelines were idle.
- Saved the former Bronze (3,140 rows), Silver (3,049 rows), and quarantine
  tables under `workspace.wikimediademo_inbox_cutover_backup_20261015` for rollback.
- Appended 1,014 historical raw Event Hubs payloads missing from inbox. Original
  JSON was preserved. Repeating preparation did not append these payloads again.
- Deployed direct-inbox pipeline code.
- Explicitly refreshed only Silver edits and quarantine. Inbox was not reset.
- Pipeline update `c795f870-6af8-463d-b4ef-51bd17c98655` completed successfully.
- Verified all expected clean source keys at the captured inbox version, all
  previous Silver keys, historical raw payload coverage, and quarantine coverage.
  All missing-record checks returned zero; duplicate Silver keys were zero.
- [Normal events job run 756111263423833](https://dbc-ab9a5afe-c151.cloud.databricks.com/jobs/498267154097880/runs/756111263423833)
  completed successfully, including the normal incremental pipeline run and
  Gold integrity/reconciliation checks.
- After verification, dropped the inactive public `bronze_stream` table.

## Final checks

| Check | Result |
| --- | ---: |
| Inbox rows | 13,140 |
| Silver clean event rows | 12,903 |
| Gold fact rows | 12,903 |
| Quarantine rows | 172 |
| Missing previous Silver keys | 0 |
| Duplicate Silver keys | 0 |

Gold retained the `user` column mask and editor-type row filter. The active
Bronze datasets are event inbox and the independent reference snapshot. The
catalog also lists Databricks-managed internal materializations, including an
inactive backing artifact from the retired Bronze dataset; these are not active
pipeline sources and were not manually modified.

The rollback schema is separate from Bronze/Silver/Gold and is not read by either
job. It preserves original transport metadata; replayed Silver processing
timestamps reflect this rebuild. Keep backups until migration acceptance, then
apply the project's retention policy.

50 relevant local tests, Flake8, `git diff --check`, and strict DEV bundle
validation passed. Schedules remain paused. Detailed migration/coverage reports
are stored locally under the ignored `.state/inbox-cutover-dev/` directory.
See [job/task responsibilities](jobs.md) and [manual cutover instructions](inbox-cutover.md).
