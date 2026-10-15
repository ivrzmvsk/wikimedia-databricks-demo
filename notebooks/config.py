# Databricks notebook source
# Shared parameters for bundle notebook tasks. Empty defaults prevent accidental
# writes to production tables when a notebook is opened manually.
def identifier(value):
    if not value:
        raise ValueError("Catalog and schema parameters must be supplied by the job.")
    return "`" + value.replace("`", "``") + "`"


def sql_literal(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


for parameter in ("catalog", "bronze_schema", "silver_schema", "gold_schema"):
    dbutils.widgets.text(parameter, "")

for parameter, default in (
    ("zerobus_service_principal", ""),
    ("data_engineers_group", "data_engineers"),
    ("admins_group", "admins"),
    ("row_admin_group", "admin_group"),
):
    dbutils.widgets.text(parameter, default)

CATALOG = identifier(dbutils.widgets.get("catalog"))
BRONZE_SCHEMA = identifier(dbutils.widgets.get("bronze_schema"))
SILVER_SCHEMA = identifier(dbutils.widgets.get("silver_schema"))
GOLD_SCHEMA = identifier(dbutils.widgets.get("gold_schema"))
BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
SILVER = f"{CATALOG}.{SILVER_SCHEMA}"
GOLD = f"{CATALOG}.{GOLD_SCHEMA}"
DATA_ENGINEERS_GROUP = sql_literal(dbutils.widgets.get("data_engineers_group"))
ADMINS_GROUP = sql_literal(dbutils.widgets.get("admins_group"))
ROW_ADMIN_GROUP = sql_literal(dbutils.widgets.get("row_admin_group"))
ZEROBUS_SERVICE_PRINCIPAL = dbutils.widgets.get("zerobus_service_principal")
