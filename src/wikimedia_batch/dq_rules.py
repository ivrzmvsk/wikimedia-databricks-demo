from pyspark.sql import DataFrame
from pyspark.sql import functions as F


BATCH_DQ_RULES = {
    "valid_wiki_code": "wiki_code IS NOT NULL",
    "valid_url": "site_url LIKE 'http%' OR site_url IS NULL"
}


def compute_dq_flags(df: DataFrame) -> DataFrame:
    result = df
    reason_columns = []

    for name, expression in BATCH_DQ_RULES.items():
        flag_column = f"_dq_{name}"
        result = result.withColumn(flag_column, F.expr(expression))
        reason_columns.append(
            F.when(~F.col(flag_column), F.lit(name)).otherwise(F.lit(None))
        )

    result = result.withColumn(
        "_dq_passed",
        F.expr(" AND ".join(f"_dq_{name}" for name in BATCH_DQ_RULES)),
    )

    return result.withColumn(
        "_dq_failure_reasons",
        F.expr("filter(array({}), reason -> reason is not null)".format(
            ", ".join(
                f"IF(NOT _dq_{name}, '{name}', NULL)"
                for name in BATCH_DQ_RULES
            )
        )),
    )