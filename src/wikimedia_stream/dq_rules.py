from pyspark.sql import DataFrame
from pyspark.sql import functions as F


DQ_RULES = {
    "valid_json": "_parse_error = false",
    "valid_event_id": "event_id IS NOT NULL",
    "valid_event_time": "event_time IS NOT NULL",
    "valid_wiki_domain": (
        "wiki IS NOT NULL AND trim(wiki) != '' "
        "AND domain IS NOT NULL AND trim(domain) != ''"
    ),
    "valid_length": (
        "(length_old IS NULL OR length_old >= 0) "
        "AND (length_new IS NULL OR length_new >= 0)"
    ),
}


def compute_dq_flags(df: DataFrame) -> DataFrame:
    result = df
    reason_columns = []

    for name, expression in DQ_RULES.items():
        flag_column = f"_dq_{name}"
        result = result.withColumn(flag_column, F.expr(expression))
        reason_columns.append(
            F.when(~F.col(flag_column), F.lit(name)).otherwise(F.lit(None))
        )

    result = result.withColumn(
        "_dq_passed",
        F.expr(" AND ".join(f"_dq_{name}" for name in DQ_RULES)),
    )

    return result.withColumn(
        "_dq_failure_reasons",
        F.expr("filter(array({}), reason -> reason is not null)".format(
            ", ".join(
                f"IF(NOT _dq_{name}, '{name}', NULL)"
                for name in DQ_RULES
            )
        )),
    )


def filter_good_records(df: DataFrame) -> DataFrame:
    return df.filter(F.col("_dq_passed"))


def filter_bad_records(df: DataFrame) -> DataFrame:
    return df.filter(~F.col("_dq_passed"))

