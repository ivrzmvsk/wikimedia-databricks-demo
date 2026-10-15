---
title: Operations Runbook - Troubleshooting, Inbox Cutover, Maintenance
topic: operations, troubleshooting
keywords: runbook, troubleshooting, failed job, inbox cutover, bronze_stream, replay, full refresh, maintenance, dim_date extension, zerobus errors, backup schema
---

# Operations Runbook

## Troubleshooting

| Symptom | Likely cause | Action |
| --- | --- | --- |
| Events job: "Gold dimensions are empty or missing their unique fallback project" | Setup or reference never ran, or the tables are not readable | Run `wikimedia_setup`, then `wikimedia_reference`; check table permissions |
| Events job: "Gold dimensions are unavailable" | `dim_*` tables missing or no access | Same as above |
| `02_dim_project` error: "Reference snapshot is empty or has duplicate/null wiki codes; keeping dim_project." | SiteMatrix snapshot invalid | Previous `dim_project` stays in place; rerun the reference job after checking the API and `silver_ref_quarantine` |
| `check_gold`: `silver_gold_reconciliation` violations | Fact table built from older Silver | Rerun `refresh_gold_edit_facts` |
| `check_gold`: `project_referential_integrity` violations | Fact rows without project row | Ensure fallback project exists; rerun reference and facts |
| `check_reference`: AssertionError | Published `dim_project` does not match active Silver | Rerun `refresh_gold_project_dimension` |
| Rows missing from Silver | Events failed DQ or are duplicates | Query `silver_wikipedia_edits_quarantine`; check `_dq_failure_reasons` |
| Producer exits with error after sending | Partial visibility or failed SQL check | Rows are not yet queryable; the manifest is kept; replay it |
| Producer: "Set ZEROBUS_CLIENT_ID and ZEROBUS_CLIENT_SECRET" | Missing credentials | Provide `--env-file` or environment variables |
| `bundle validate` fails on missing variable | `warehouse_id` not set | `export BUNDLE_VAR_warehouse_id=<id>` |
| Policy changes not visible on fact table | Governance not re-run | Rerun the `prepare_gold_fact_and_policies` setup task |

## Replaying events

```bash
.venv/bin/python producer/zerobus/producer.py --target ivan_personal \
  --env-file producer/zerobus/.env.personal --replay --manifest .state/<manifest>.json
```

Replay sends the exact payloads and source IDs again. It rejects a manifest
created for a different target or table. Do not edit manifests; they contain
public Wikimedia usernames, so store them according to your retention needs.
Replay can create duplicate inbox rows; Silver deduplicates them by event ID.

## Routine maintenance

- Pause processing schedules while rerunning setup (static dimensions are rebuilt).
- Extend `dim_date` (`notebooks/gold/01_dim_date.py`, currently up to 2027-12-31) before 2028.
- Configure `job_email_notifications` and `alert_notifications.subscriptions`
  so failures reach people.
- Fact table full rebuild happens on every events run; no manual action needed.

## One-time inbox cutover for deployments that still have `bronze_stream`

Only for older deployments. New installations skip this.
Do not run an ordinary pipeline update against old checkpoints, and never refresh
from an incomplete inbox.

1. Keep schedules paused and confirm jobs and pipelines are idle.
2. Run `notebooks/migrations/01_prepare_inbox_cutover.py` manually with the usual
   catalog/schema parameters and a dedicated `backup_schema`. It saves the old
   Bronze, Silver and quarantine outside the active schemas and appends only
   raw payloads missing from the inbox (exact original JSON preserved).
3. Deploy the new pipeline definition, which reads the inbox directly.
4. Refresh **only** `silver_wikipedia_edits` and `silver_wikipedia_edits_quarantine`
   so data and checkpoints are rebuilt from the full inbox. Never include the
   inbox itself in a refresh.
5. Verify: all previous clean event keys remain, all expected clean inbox keys are
   present, no duplicate keys, raw/quarantine coverage complete. For large
   backfills consider event-time ordering and the 10-minute watermark.
6. Run the regular events job to rebuild Gold and run its checks; confirm masks
   and row filters are still attached to the fact table.
7. Only after verification drop the inactive old `bronze_stream`. Keep the backup
   schema until the migration is accepted, then remove it per retention policy.

Backup tables are rollback artifacts, not a Bronze layer. Replayed Silver rows
receive new processing timestamps; event payloads, event timestamps and source
event IDs remain unchanged.

## Validation commands after a change

```bash
.venv/bin/python -m pytest -q
.venv/bin/flake8 . --count --show-source --statistics
databricks bundle validate -t ivan_personal --strict
```

## FAQ

**Can I run the events job without the producer?** Yes; it processes whatever is already in the inbox.
**How do I reprocess everything?** Refresh only the Silver streaming tables (never the inbox) and then rerun the events job.
**Where do I find a failed task's error?** In the Jobs UI run page for the job, or `databricks jobs get-run <run_id>`.
