from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from wikimedia_stream.schema import WIKIPEDIA_EVENT_SCHEMA


def add_bronze_metadata(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("_source", F.lit("azure_eventhub_wikimedia_recentchange"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_load_date", F.current_date())
    )


def decode_kafka_events(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("value").cast("string").alias("event_json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_enqueued_at"),
    )


def parse_bronze_to_silver(df: DataFrame) -> DataFrame:
    parse_schema = StructType(
        WIKIPEDIA_EVENT_SCHEMA.fields
        + [StructField("_corrupt_record", StringType())]
    )

    parsed = (
        df.withColumn(
            "event",
            F.from_json(
                F.col("event_json"),
                parse_schema,
                {"columnNameOfCorruptRecord": "_corrupt_record"},
            ),
        )
        .withColumn("_parse_error", F.col("event._corrupt_record").isNotNull())
    )

    return (
        parsed.select(
            F.col("event.id").alias("event_id"),
            F.col("event.type").alias("event_type"),
            F.col("event.namespace").alias("namespace"),
            F.col("event.title").alias("title"),
            F.col("event.title_url").alias("title_url"),
            F.col("event.comment").alias("comment"),
            F.col("event.timestamp").alias("event_timestamp_unix"),
            F.from_unixtime(F.col("event.timestamp")).cast("timestamp").alias("event_time"),
            F.to_timestamp(F.col("event.meta.dt")).alias("event_time_utc"),
            F.col("event.user").alias("user"),
            F.col("event.bot").alias("bot"),
            F.col("event.minor").alias("minor"),
            F.col("event.patrolled").alias("patrolled"),
            F.col("event.server_name").alias("server_name"),
            F.col("event.wiki").alias("wiki"),
            F.col("event.meta.id").alias("meta_id"),
            F.col("event.meta.domain").alias("domain"),
            F.col("event.length.old").alias("length_old"),
            F.col("event.length.new").alias("length_new"),
            F.col("event._producer.source").alias("producer_source"),
            F.to_timestamp(F.col("event._producer.producer_timestamp_utc")).alias(
                "producer_timestamp_utc"
            ),
            F.col("_parse_error"),
            F.col("event_json"),
            F.col("kafka_topic"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
            F.col("kafka_enqueued_at"),
            F.col("_source"),
            F.col("_ingested_at").alias("bronze_ingested_at"),
            F.col("_load_date").alias("bronze_load_date"),
        )
        .withColumn(
            "editor_type",
            F.when(F.col("bot") == F.lit(True), F.lit("bot")).otherwise(F.lit("human")),
        )
        .withColumn("byte_change", F.col("length_new") - F.col("length_old"))
        .withColumn("abs_byte_change", F.abs(F.col("byte_change")))
        .withColumn("silver_ingested_at", F.current_timestamp())
        .withColumn("silver_load_date", F.current_date())
    )
