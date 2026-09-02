from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)


WIKIPEDIA_EVENT_SCHEMA = StructType(
    [
        StructField("id", LongType()),
        StructField("type", StringType()),
        StructField("namespace", LongType()),
        StructField("title", StringType()),
        StructField("title_url", StringType()),
        StructField("comment", StringType()),
        StructField("timestamp", LongType()),
        StructField("user", StringType()),
        StructField("bot", BooleanType()),
        StructField("minor", BooleanType()),
        StructField("patrolled", BooleanType()),
        StructField("server_name", StringType()),
        StructField("wiki", StringType()),
        StructField(
            "meta",
            StructType(
                [
                    StructField("id", StringType()),
                    StructField("domain", StringType()),
                    StructField("dt", StringType()),
                ]
            ),
        ),
        StructField(
            "length",
            StructType(
                [
                    StructField("old", LongType()),
                    StructField("new", LongType()),
                ]
            ),
        ),
        StructField(
            "_producer",
            StructType(
                [
                    StructField("source", StringType()),
                    StructField("producer_timestamp_utc", StringType()),
                ]
            ),
        ),
    ]
)

