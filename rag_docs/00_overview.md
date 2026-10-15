---
title: Wikimedia Databricks Lakehouse - Project Overview and Glossary
topic: overview
keywords: wikimedia, lakehouse, databricks, medallion, bronze, silver, gold, zerobus, unity catalog, glossary
---

# Wikimedia Databricks Lakehouse - Project Overview

## What is this project?

The Wikimedia Databricks Lakehouse Demo processes the live Wikimedia
"recent change" stream (edits made on Wikipedia and sister projects) and
Wikimedia reference data (the list of all wiki projects) on Azure Databricks.
It uses Delta Lake, Unity Catalog, Lakeflow Spark Declarative Pipelines
(formerly Delta Live Tables) and Databricks Asset Bundles (DABs).

The result is an analytics-ready star schema in the Gold layer, for example
"how many edits did bots vs humans make per language", plus data quality
monitoring and a RAG assistant over Wikipedia article summaries.

## Technology stack

- Azure Databricks (serverless jobs and serverless Lakeflow pipelines)
- Delta Lake tables governed by Unity Catalog
- Databricks Zerobus Ingest (gRPC, writes directly to a Delta table)
- Lakeflow Spark Declarative Pipelines (`pyspark.pipelines`, imported as `dp`)
- Databricks Asset Bundles for deployment (`databricks.yml`, `resources/*.yml`)
- Python 3.12, PySpark, pytest, flake8
- GitHub Actions for CI/CD

## Repository layout

| Path | Purpose |
| --- | --- |
| `databricks.yml` | Bundle targets, variables, sync paths |
| `resources/pipelines.yml` | Two Lakeflow pipelines (streaming and batch) |
| `resources/workflow.yml` | Three jobs: setup, events, reference |
| `resources/alerts.yml` | Three SQL alerts |
| `pipelines/` | Lakeflow dataset definitions (Silver layer) |
| `src/wikimedia_stream/` | Schema, DQ rules, dedup, parsing for events |
| `src/wikimedia_batch/` | SiteMatrix fetch and DQ rules for reference data |
| `src/wikimedia_rag/` | Corpus, enrichment, storage, search for the RAG assistant |
| `notebooks/` | Parameterized setup, Gold, governance and check notebooks |
| `producer/zerobus/` | External Python producer that sends events through Zerobus |
| `rag/` | Three small bundles: preparation, serving (index), application (App) |
| `tests/` | PySpark and bundle configuration tests |
| `docs/` | Operational documents |

The folders `00-setup/` to `05_bronze_dashboard_analytics/` and the Event Hubs
producer in `producer/producer.py` are historical manual notebooks. They are
not part of the deployed bundle.

## Glossary

- **Medallion architecture**: data is refined through Bronze (raw), Silver
  (cleaned) and Gold (analytics model) layers.
- **Bronze**: raw data as received. Here: `zerobus_wikimedia_inbox` (events)
  and `ref_wikimedia_projects` (reference).
- **Silver**: parsed, validated, deduplicated data plus quarantine tables.
- **Gold**: star schema (facts and dimensions) for analytics.
- **Quarantine**: a table that holds records that failed data quality rules,
  so bad data is kept for inspection instead of being lost.
- **DQ**: data quality. Rules are in `src/wikimedia_stream/dq_rules.py`.
- **Zerobus**: Databricks service that ingests records directly into a Unity
  Catalog Delta table over gRPC without a message bus such as Kafka.
- **Inbox**: the Bronze table `zerobus_wikimedia_inbox` that Zerobus writes.
- **SCD2**: Slowly Changing Dimension type 2. Keeps history of changes using
  `__START_AT` and `__END_AT`; the current row has `__END_AT IS NULL`.
- **SiteMatrix**: Wikimedia API listing all wiki projects (language, name, URL).
- **Wiki code / dbname**: project identifier such as `enwiki`, `dewiki`,
  `commonswiki`, `wikidatawiki`.
- **Fallback project**: a Gold `dim_project` row with `wiki_code = 'unknown'`
  used for events whose wiki is not in the reference data.
- **Bundle target**: a named deployment environment in `databricks.yml`:
  `ivan_personal` and `azure_trial_2`.
- **Fact / dimension**: fact table stores events (`fact_wikipedia_edits`);
  dimensions describe them (`dim_date`, `dim_project`, `dim_editor_type`).
