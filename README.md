# Wikimedia Databricks Lakehouse Demo

[![Wikimedia CI Pipeline](https://github.com/ivrzmvsk/wikimedia-databricks-demo/actions/workflows/ci.yml/badge.svg)](https://github.com/ivrzmvsk/wikimedia-databricks-demo/actions/workflows/ci.yml)

An end-to-end Big Data streaming and batch processing pipeline built on **Azure Databricks**, **Delta Lake**, and the **Medallion Architecture**, ingesting and analyzing live Wikimedia event streams.

---

## 🚀 Key Features

- **Streaming & Batch Ingestion:** Real-time ingestion of Wikimedia RecentChange streams via Azure Event Hubs / Kafka protocols.
- **Medallion Architecture:** Multi-hop data lakehouse design:
  - **Bronze:** Raw JSON events preserved with ingestion timestamps and metadata.
  - **Silver:** Cleaned, schema-enforced, and deduplicated Wikimedia edit records.
  - **Gold:** Curated analytical tables and aggregated dimensional models for business metrics.
- **Data Quality & Constraints:** Schema enforcement, null-checks, title formatting, and business rule validations.
- **Automated CI Workflow:** Continuous Integration powered by GitHub Actions for code linting and PySpark transformations validation.

---

## 🧪 Testing & CI Pipeline

The project employs an automated CI workflow (`.github/workflows/ci.yml`) executed on every Pull Request and push to `main`:

- **Linting:** `flake8` syntax and code style validation configured with Databricks builtins (`spark`, `dbutils`).
- **Unit & Transformation Tests:** PySpark suite executed via `pytest` and `chispa`:
  - Deduplication key validation and content hash fallback.
  - Malformed payload isolation and JSON schema compliance.
  - Domain, title cleansing, and length DQ rule assertions.
  - Schema evolution and partition integrity checks.

---

## 📁 Project Structure

```text
├── .github/workflows/          # CI/CD automation pipelines
├── pipelines/                  # Streaming & batch medallion pipelines
├── src/                        # Core transformation logic and utility scripts
├── tests/                      # Automated PySpark and pytest test suites
├── databricks.yml              # Databricks Asset Bundle (DAB) configuration
└── README.md                   # Project documentation
