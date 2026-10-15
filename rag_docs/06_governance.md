---
title: Unity Catalog Governance - Column Mask, Row Filter, Groups and Permissions
topic: governance, security
keywords: unity catalog, column mask, row filter, mask_user_name, filter_active_edits, data_engineers, admins, admin_group, is_account_group_member, grants, zerobus service principal
---

# Unity Catalog Governance

Governance is applied to `fact_wikipedia_edits` by `notebooks/gold/05_unity_catalog_governance.py`,
which the setup task `prepare_gold_fact_and_policies` runs through `setup_gold.py`.

## Column mask on user names

Function `<catalog>.<gold_schema>.mask_user_name(user_name STRING)`:

- Members of the account group `data_engineers` or `admins` see the real value.
- Everyone else sees the first two characters followed by `***` (for example `Jo***`).

It is attached with `ALTER TABLE ... ALTER COLUMN user SET MASK mask_user_name`.
Editor names can be personal data (usernames or IP addresses), so masking is the default.

## Row filter

Function `<catalog>.<gold_schema>.filter_active_edits(editor_type_key INT)`:

- Members of the account group `admin_group` see all rows.
- Everyone else sees rows where `editor_type_key IN (1, 2)`, i.e. bot and human edits.

Attached with `ALTER TABLE ... SET ROW FILTER filter_active_edits ON (editor_type_key)`.

## Group names (configurable)

| Bundle variable | Default | Used for |
| --- | --- | --- |
| `data_engineers_group` | `data_engineers` | Unmasked user names |
| `admins_group` | `admins` | Unmasked user names |
| `row_admin_group` | `admin_group` | Row filter bypass |

The groups must already exist as account groups; the bundle does not create them.

## Policy lifecycle

- Setup attaches the mask and filter while the fact table is **empty**, before
  the first events run publishes data.
- Later events runs use `CREATE OR REPLACE TABLE`. Databricks retains masks and
  row filters on replacement as long as the column names stay the same.
- Changing a policy requires rerunning the setup governance task:
  `databricks bundle run -t <target> wikimedia_setup --only prepare_gold_fact_and_policies`.

## Producer permissions

When the bundle variable `zerobus_service_principal` (OAuth application ID) is
set, the setup task `create_event_inbox` runs:

- `GRANT USE CATALOG ON CATALOG <catalog>`
- `GRANT USE SCHEMA ON SCHEMA <bronze_schema>`
- `GRANT SELECT, MODIFY ON TABLE <bronze_schema>.zerobus_wikimedia_inbox`

The deployment identity must be allowed to grant these. If permissions are
managed externally, leave the variable empty and grant them separately.

## RAG assistant permissions

The Databricks App service principal receives only `SELECT` on the vector search
index and `CAN_QUERY` on the embedding and chat model serving endpoints. It
gets no access to Silver, the summary cache, write permissions or a SQL warehouse.
Do not grant `ALL PRIVILEGES` to work around grant failures; ask the schema or
index owner to assign the exact rights.

## FAQ

**Why do I see `Jo***` instead of usernames?** You are not in `data_engineers` or `admins`.
**Who sees all rows?** Members of `admin_group` (variable `row_admin_group`).
**Does rebuilding the fact table remove the mask?** No, as long as column names are unchanged.
