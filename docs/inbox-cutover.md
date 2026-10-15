# One Bronze table for event streaming

The steady-state path is:

```text
Wikimedia → external producer → Zerobus → Bronze inbox
                                           ↓
                              Silver edits + quarantine → Gold facts
```

`zerobus_wikimedia_inbox` is the sole persisted event Bronze table. Its adapter
adds metadata in the temporary Silver staging view; it does not copy raw JSON
into another table. The existing streaming pipeline ID and Silver output names
are retained. The batch reference pipeline is unaffected.

## Existing deployments

Changing the Delta source requires a one-time migration. Do not run an ordinary
pipeline update against the old checkpoints or refresh from an incomplete inbox.

1. Keep schedules paused and confirm jobs and pipelines are idle.
2. Run `notebooks/migrations/01_prepare_inbox_cutover.py` manually with the usual
   catalog/schema parameters and a dedicated `backup_schema`. It saves the old
   Bronze, Silver and quarantine outside the active medallion schemas, then
   appends only raw payloads missing from inbox. Exact original JSON is retained.
3. Deploy the new pipeline definition. It reads inbox directly.
4. Explicitly refresh **only** `silver_wikipedia_edits` and
   `silver_wikipedia_edits_quarantine`, so their data and checkpoints are rebuilt
   from the complete inbox. Never include the external inbox in a refresh.
5. Verify that all previous clean event keys remain, all expected clean inbox
   keys are present, no duplicate keys exist, and raw/quarantine coverage is
   complete. For a large backfill, plan event-time ordering and the watermark:
   historical records arriving in later microbatches can otherwise be dropped.
6. Run the regular events job to rebuild Gold and execute its checks. Check
   preservation of existing masks and row filters on the Gold fact table.
7. Only after verification, drop the inactive old `bronze_stream` table. Keep the
   backup schema until the migration has been accepted, then remove it under
   the project's retention policy.

The backup tables are rollback artifacts, not another Bronze layer or pipeline
source. They preserve original transport/processing metadata; replayed Silver
rows receive new processing timestamps and the inbox adapter's transport columns.
Event payloads, event timestamps and stable source event IDs remain unchanged.

New installations only need [manual setup](manual-setup.md); no cutover or
historical backup is necessary. There are two regular processing jobs and an independent manual setup job.
The cutover is not a recurring task in any job.
