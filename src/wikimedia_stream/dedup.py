from pyspark.sql import DataFrame
from pyspark.sql import functions as F


DEDUP_KEY_COLUMN = "_wikimedia_event_key"
DEDUP_KEY_COLUMNS = [DEDUP_KEY_COLUMN]
WATERMARK_COLUMN = "event_time"
WATERMARK_THRESHOLD = "10 minutes"


def add_dedup_key(df: DataFrame) -> DataFrame:
    fallback_key = F.sha2(
        F.concat_ws(
            "|",
            F.coalesce(F.col("wiki"), F.lit("")),
            F.coalesce(F.col("event_id").cast("string"), F.lit("")),
            F.coalesce(F.col("event_timestamp_unix").cast("string"), F.lit("")),
            F.coalesce(F.col("event_json"), F.lit("")),
        ),
        256,
    )

    return df.withColumn(
        DEDUP_KEY_COLUMN,
        F.coalesce(F.col("meta_id"), fallback_key),
    )


def deduplicate_records(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(DEDUP_KEY_COLUMNS)


def deduplicate_stream_records(df: DataFrame) -> DataFrame:
    return df.withWatermark(WATERMARK_COLUMN, WATERMARK_THRESHOLD).dropDuplicates(
        DEDUP_KEY_COLUMNS
    )
