---
title: Gold Layer - Star Schema, Tables, Columns and Analytics Queries
topic: gold, data model, analytics
keywords: fact_wikipedia_edits, dim_project, dim_date, dim_editor_type, star schema, project_key, date_key, editor_type_key, byte_change, fallback project, language_name, dashboard queries
---

# Gold Layer - Star Schema

All Gold tables live in `<catalog>.<gold_schema>` (default schema
`wikimediademo_gold`). They are rebuilt with `CREATE OR REPLACE TABLE`.

## fact_wikipedia_edits

Grain: one row per deduplicated clean Wikimedia event from `silver_wikipedia_edits`.
Built by `notebooks/gold/04_fact_wikipedia_edits.py`. The empty table is created
once by `notebooks/setup_gold.py`; later runs replace its content and keep its policies.

| Column | Type | Description |
| --- | --- | --- |
| `event_id` | BIGINT | Wikimedia recent-change ID |
| `date_key` | INT | `yyyyMMdd` of `coalesce(event_time, silver_ingested_at)`; joins `dim_date` |
| `project_key` | STRING | `md5(wiki)`; joins `dim_project`; falls back to `md5('unknown')` |
| `editor_type_key` | INT | 1 = bot, 2 = human; joins `dim_editor_type` |
| `user` | STRING | Editor name (masked for non-privileged users) |
| `title` | STRING | Page title |
| `event_time` | TIMESTAMP | Event time |
| `length_old` | BIGINT | Page bytes before |
| `length_new` | BIGINT | Page bytes after |
| `byte_change` | BIGINT | `length_new - length_old` |
| `abs_byte_change` | BIGINT | `abs(length_new - length_old)` |
| `is_minor_edit` | BOOLEAN | `coalesce(minor, false)` |
| `gold_ingested_at` | TIMESTAMP | Time of the Gold rebuild |

Join rule: `LEFT JOIN dim_project p ON md5(coalesce(s.wiki,'unknown')) = p.project_key`.
Events whose wiki is not in `dim_project` get the fallback project key, so the
fact table has no orphaned project keys.

Before rebuilding, the notebook runs a **preflight** check. It raises an error
(and does not replace the fact table) if `dim_date`, `dim_editor_type` or
`dim_project` is empty/unavailable, or if there is not exactly one fallback
project. The error message tells the operator to run `wikimedia_setup` and
`wikimedia_reference` first.

## dim_project

Built by `notebooks/gold/02_dim_project.py` from the active rows
(`__END_AT IS NULL`) of `silver_ref_wikimedia_projects`.

| Column | Description |
| --- | --- |
| `project_key` | `md5(coalesce(wiki_code,'unknown'))` |
| `wiki_code` | e.g. `enwiki` |
| `language_code` | e.g. `en`; `unknown` if missing |
| `language_name` | Readable language from a large CASE mapping (e.g. `en` -> English, `de` -> German); unmapped codes become `ISO: <CODE>`; `unknown` becomes `Other / Technical` |
| `project_name` | e.g. `Wikipedia`; defaults to `Wikipedia` |
| `site_url` | Project URL |
| `gold_updated_at` | Publication time (used by the freshness alert) |

Publication safeguards: the notebook raises an error and keeps the previous
dimension if the Silver snapshot is empty or has duplicate/null `wiki_code`
values. The fallback row (`unknown`, `Other Wikimedia Project`,
`https://www.wikimedia.org`) is added with `UNION ALL` in the same statement, so
the event job never sees a dimension without it.

## dim_date

Built by `01_dim_date.py` (run by the setup job). Calendar from 2025-01-01 to
2027-12-31. Columns: `date_key` (yyyyMMdd INT), `calendar_date`, `year`,
`quarter`, `month`, `month_name`, `day`, `day_of_week` (1 = Sunday),
`day_name`, `is_weekend` (Sunday or Saturday). The range must be extended before 2028.

## dim_editor_type

| editor_type_key | is_bot | editor_type_desc |
| --- | --- | --- |
| 1 | true | Bot |
| 2 | false | Human |

## Example analytics queries

(`notebooks/gold/06_dashboard_queries.py`; they are manual SELECTs, not part of any job.)

Top languages by volume and bot activity:

```sql
SELECT p.language_name, p.project_name,
       SUM(CASE WHEN e.editor_type_desc = 'Bot'   THEN 1 ELSE 0 END) AS bot_edits,
       SUM(CASE WHEN e.editor_type_desc = 'Human' THEN 1 ELSE 0 END) AS human_edits,
       COUNT(f.event_id) AS total_edits,
       SUM(f.abs_byte_change) AS total_bytes_altered
FROM fact_wikipedia_edits f
JOIN dim_project p ON f.project_key = p.project_key
JOIN dim_editor_type e ON f.editor_type_key = e.editor_type_key
GROUP BY 1, 2 ORDER BY total_edits DESC LIMIT 40;
```

Bot vs human behavior: average absolute byte change and share of minor edits per
`editor_type_desc`. Edits timeline: `date_trunc('minute', event_time)` grouped by editor type.

## FAQ

**What is the primary key of the fact table?**
`event_id` identifies the event. Silver deduplication uses `meta_id`.

**How do I count bot edits?**
Join `dim_editor_type` and filter `editor_type_desc = 'Bot'`, or `editor_type_key = 1`.

**Why does a fact row have project `unknown`?**
Its `wiki` value was missing or not in the reference data.
