# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

spark.sql(f"""
-- ----------------------------------------------------------------------------
-- 1. Column-Level Security (CLS): Masking user handles
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION {GOLD}.mask_user_name(user_name STRING)
RETURN IF(
    is_account_group_member({DATA_ENGINEERS_GROUP}) OR is_account_group_member({ADMINS_GROUP}),
    user_name,
    concat(substring(user_name, 1, 2), '***')
)
""")

# COMMAND ----------

spark.sql(f"""
-- Apply column mask to the user column in the Gold fact table
ALTER TABLE {GOLD}.fact_wikipedia_edits
ALTER COLUMN user SET MASK {GOLD}.mask_user_name
""")

# COMMAND ----------

spark.sql(f"""
-- ----------------------------------------------------------------------------
-- 2. Row-Level Security (RLS): Row access filter
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION {GOLD}.filter_active_edits(editor_type_key INT)
RETURN IF(
    is_account_group_member({ROW_ADMIN_GROUP}),
    true,
    editor_type_key IN (1, 2)
)
""")

# COMMAND ----------

spark.sql(f"""
-- Apply row filter
ALTER TABLE {GOLD}.fact_wikipedia_edits
SET ROW FILTER {GOLD}.filter_active_edits ON (editor_type_key)
""")
