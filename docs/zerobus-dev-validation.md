# Zerobus DEV validation — 2026-10-15

Target: `ivan_personal`; profile: `personal-env`; catalog: `workspace`.
The existing deployment, pipeline IDs and `wikimediademo_*` schemas were reused.
No parallel Event Hubs pipeline or replacement set of schemas was created.
Azure was not deployed or run.

## Execution

- Strict bundle validation passed.
- Deployment plan: 0 resources added, 2 updated, 0 deleted.
- Existing job: `498267154097880`.
- Existing streaming pipeline: `6eb6d32f-21af-44d1-8223-d15c2dc34733`.
- Setup run `1117737893207420` succeeded, creating the managed inbox and grants.
- Captured 100 real Wikimedia events and sent them with 20 duplicate deliveries.
- Zerobus flush/close succeeded; all 120 inbox payloads and producer timestamps
  matched the saved manifest exactly.
- [Full workflow run 818029893790988](https://dbc-ab9a5afe-c151.cloud.databricks.com/jobs/498267154097880/runs/818029893790988?o=7474655630219513)
  completed successfully, including both Lakeflow pipelines, dimensions, facts,
  Gold integrity/reconciliation checks, governance and analytics.
- Streaming update: `966f0110-51ae-4ba8-b686-1e027a6b1b4a`.

## Results

| Table | Before | New rows | After |
| --- | ---: | ---: | ---: |
| `wikimediademo_bronze.zerobus_wikimedia_inbox` | 0 | 120 | 120 |
| `wikimediademo_bronze.bronze_stream` | 1,014 | 120 | 1,134 |
| `wikimediademo_silver.silver_wikipedia_edits` | 970 | 99 | 1,069 |
| `wikimediademo_silver.silver_wikipedia_edits_quarantine` | 0 | 1 | 1 |
| `wikimediademo_gold.fact_wikipedia_edits` | 970 | 99 | 1,069 |

All 100 unique source IDs were accounted for in clean Silver or quarantine.
The quarantined record was a `log` event with no `event_id`; it failed the
existing `valid_event_id` rule. There were zero duplicate event keys in Silver.

Historical Event Hubs row counts remained exactly 1,014 in Bronze and 970 in
Silver. Both backing table UUIDs remained unchanged. The new named append flow
preserved the tables and started a separate checkpoint for the Delta inbox;
no full refresh was used. The active pipeline has no Event Hubs configuration.

Final SQL verification statement: `01f1c82d-edd6-1219-9bd7-a3c072ac445f`.
Detailed reports and the replay manifest are retained locally under the ignored
`.state/zerobus-dev-validation/` directory. OAuth env files and local bundle
variable overrides are Git-ignored; no secrets are included in this report.

## Local checks and operating mode

All 42 local tests passed; Flake8 and `git diff --check` passed.
Workflow and alert schedules remain paused. The producer is currently a manual,
bounded capture command; the validation did not start a permanent producer.

The active processing path is:

```text
Wikimedia → Zerobus → workspace.wikimediademo_bronze.zerobus_wikimedia_inbox
                    → existing Bronze → existing Silver → existing Gold
```

## Visibility and capture deadline follow-up

The producer now confirms queryability separately from Zerobus durability.
It polls SQL for all distinct manifest IDs before returning success. The
documented `producer && workflow` command stops before downstream execution on
partial visibility, SQL failure or visibility timeout. SQL polling is
asynchronous and unfinished statements are cancelled on timeout.

SSE capture runs in a child process controlled by the parent deadline, so a
connection that only emits heartbeat comments cannot keep the capture alive.
The regression test uses a real local HTTP SSE server with continuous comments,
checks the timeout, and verifies that no child process remains.

- All 52 local tests passed, including delayed/partial Delta visibility, pending
  SQL cancellation, late SQL results and heartbeat-only capture. Lint passed.
- A fresh personal DEV capture sent 5 real events and 1 duplicate. The CLI
  reported both `durability_confirmed: true` and `queryable_confirmed: true`,
  with `visible_unique_events: 5`, before the shell started downstream.
- [Workflow run 667206744294497](https://dbc-ab9a5afe-c151.cloud.databricks.com/jobs/498267154097880/runs/667206744294497?o=7474655630219513)
  completed successfully, including all Gold checks and governance.
- A SQL check scoped to those exact manifest IDs found 6 inbox deliveries,
  6 Bronze deliveries, 5 Silver events, no new quarantine records and zero
  duplicate keys in Silver. Statement: `01f1c82f-b2c1-130d-853a-e57a9b67ba66`.

Follow-up evidence is kept under the ignored
`.state/zerobus-readiness-validation/` directory.
