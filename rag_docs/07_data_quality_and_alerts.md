---
title: Data Quality Checks and SQL Alerts
topic: data quality, monitoring, alerts
keywords: check_gold, check_reference, referential integrity, reconciliation, quarantine alert, freshness alert, stale reference, sql alert, retrigger, notification
---

# Data Quality Checks and Alerts

## Job-level checks (fail the job on violation)

### check_gold.py (events job, task `validate_gold_edit_facts`)

| Check | Violation when |
| --- | --- |
| `unique_project_keys` | A `project_key` appears more than once in `dim_project` |
| `project_referential_integrity` | A fact row has no matching `dim_project` row |
| `date_referential_integrity` | A fact row has no matching `dim_date` row |
| `silver_gold_reconciliation` | `ABS(count(silver_wikipedia_edits) - count(fact_wikipedia_edits)) > 0` |

If any check reports violations, the notebook raises `AssertionError("Gold quality checks failed: ...")`
so the job cannot report success.

### check_reference.py (reference job, task `validate_gold_project_dimension`)

Raises `AssertionError` unless all hold:
- active Silver projects (`__END_AT IS NULL`) are not zero,
- published projects (excluding `unknown`) equal source projects,
- distinct `project_key` count equals published projects plus 1 (the fallback),
- exactly one fallback project exists,
- no active Silver project is missing from `dim_project`.

### Preflight in the fact notebook

Before rebuilding the fact table, `04_fact_wikipedia_edits.py` verifies that
`dim_date`, `dim_editor_type` and `dim_project` are populated and that exactly
one fallback project exists. On failure it raises an error instructing the
operator to run `wikimedia_setup` and `wikimedia_reference`; the existing fact
table is not replaced.

### Pipeline expectations

Lakeflow `expect_all_or_drop` drops rows failing the DQ rules (events: five
rules, reference: two rules). Dropped event rows are also written to the
quarantine tables. See the Bronze/Silver document for the rules.

## SQL alerts (resources/alerts.yml)

All alerts run on the SQL warehouse given by the variable `warehouse_id`, use
`alert_notifications` (default: `retrigger_seconds: 3600`, no subscribers), and
follow `schedule_pause_status` (default `PAUSED`). Time zone Europe/Warsaw.

| Alert (display name) | Query idea | Fires when | Schedule |
| --- | --- | --- | --- |
| `wikimedia_quarantine_<target>` | Count of rows in `silver_wikipedia_edits_quarantine` with `silver_ingested_at` in the last hour | value > 0 | minute 15 of every hour |
| `wikimedia_reference_reconciliation_<target>` | Count of Bronze `ref_wikimedia_projects` rows with no active Silver row | value > 0 | minute 30 of every hour |
| `wikimedia_reference_freshness_<target>` | 1 if `MAX(gold_updated_at)` of `dim_project` (excluding `unknown`) is NULL or older than 48 hours, else 0 | value > 0 | minute 45 of every hour |

The freshness alert never blocks event processing; it only notifies.
Alert notification recipients must be configured through the `alert_notifications`
variable (`subscriptions` list), otherwise no one is notified.

The historical exports in `alerts/` (for example `dq_quarantine_rate_query.sql`
and `*.dbalert.json`) are superseded by `resources/alerts.yml`.

## FAQ

**The quarantine alert fired. What now?** Query the quarantine table grouped by `_dq_failure_reasons` for the last hour.
**The freshness alert fired.** Run or investigate the `wikimedia_reference` job; check SiteMatrix availability.
**Why is the silver/gold reconciliation failing?** The fact table was built from an older Silver state; rerun `refresh_gold_edit_facts`.
