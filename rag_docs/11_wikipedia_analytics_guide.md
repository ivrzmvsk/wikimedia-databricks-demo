---
title: Analytics Guide - Metrics and SQL Recipes over the Wikipedia Edit Data
topic: analytics, sql recipes, metrics
keywords: metrics, bot share, edit volume, byte change, minor edit ratio, top pages, hourly activity, language ranking, sql recipes, fact_wikipedia_edits
---

# Analytics Guide

Use these recipes on the Gold tables in `<catalog>.wikimediademo_gold`.
Table and column meanings are in the Gold model document.

## Metric definitions

| Metric | Definition |
| --- | --- |
| Total edits | `COUNT(event_id)` in `fact_wikipedia_edits` |
| Bot share | Edits with `editor_type_key = 1` divided by total edits |
| Average edit size | `AVG(abs_byte_change)` |
| Net content change | `SUM(byte_change)`; positive means growth |
| Minor edit ratio | Share of rows where `is_minor_edit = true` |
| Edits per minute | Rows grouped by `date_trunc('minute', event_time)` |
| Active pages | `COUNT(DISTINCT title)` |

The fact table contains the recent-change event types delivered by the producer
after DQ filtering; the event stream also contains page creations and log
actions, so counts represent change events rather than only article revisions.
Column `user` is masked for users outside `data_engineers` and `admins`.

## Recipes

Bot share by language:

```sql
SELECT p.language_name,
       COUNT(*) AS edits,
       ROUND(100.0 * SUM(CASE WHEN f.editor_type_key = 1 THEN 1 ELSE 0 END) / COUNT(*), 1) AS bot_pct
FROM fact_wikipedia_edits f
JOIN dim_project p ON f.project_key = p.project_key
GROUP BY 1 ORDER BY edits DESC;
```

Most edited pages:

```sql
SELECT p.wiki_code, f.title, COUNT(*) AS edits, SUM(f.byte_change) AS net_bytes
FROM fact_wikipedia_edits f
JOIN dim_project p ON f.project_key = p.project_key
GROUP BY 1, 2 ORDER BY edits DESC LIMIT 20;
```

Largest single edits:

```sql
SELECT event_time, title, byte_change
FROM fact_wikipedia_edits
ORDER BY abs_byte_change DESC LIMIT 20;
```

Activity per hour of day:

```sql
SELECT hour(event_time) AS hour_of_day, COUNT(*) AS edits
FROM fact_wikipedia_edits GROUP BY 1 ORDER BY 1;
```

Weekend vs weekday:

```sql
SELECT d.is_weekend, COUNT(*) AS edits
FROM fact_wikipedia_edits f JOIN dim_date d ON f.date_key = d.date_key
GROUP BY 1;
```

Events routed to the fallback project (unknown wiki):

```sql
SELECT COUNT(*) FROM fact_wikipedia_edits f
JOIN dim_project p ON f.project_key = p.project_key
WHERE p.wiki_code = 'unknown';
```

Pipeline health: data quality failures by reason

```sql
SELECT explode(_dq_failure_reasons) AS reason, COUNT(*) AS rejected
FROM <catalog>.wikimediademo_silver.silver_wikipedia_edits_quarantine
GROUP BY 1 ORDER BY rejected DESC;
```

## Notes for interpretation

- `event_time` derives from the Unix `timestamp` of the Wikimedia event; `event_time_utc`
  (Silver only) derives from `meta.dt`.
- Volumes depend on how long and how many events the producer captured; it is a
  bounded command, not a continuous feed.
- `byte_change` is NULL whenever `length_old` or `length_new` is NULL.
