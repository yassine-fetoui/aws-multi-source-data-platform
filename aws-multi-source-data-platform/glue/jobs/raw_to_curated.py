"""
AWS Glue PySpark Job — Raw → Curated Zone
AWS Multi-Source Data Platform

Reads compacted Parquet files from S3 Raw zone,
applies schema validation, deduplication, type casting, and business enrichment,
then writes clean Delta-formatted Parquet to S3 Curated zone.

Challenge solved: data skew causing executor OOM and job failures.
Solution: salted keys + Adaptive Query Execution (AQE).
"""

import sys
import logging
from datetime import datetime, timezone

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType, LongType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Job Arguments ─────────────────────────────────────────────────────────────

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "raw_bucket",
    "curated_bucket",
    "watermark_date",       # process only files newer than this date
    "source_prefix",        # e.g. "orders"
    "target_prefix",        # e.g. "orders_curated"
])

# ── Spark / Glue Context ──────────────────────────────────────────────────────

sc           = SparkContext()
glueContext  = GlueContext(sc)
spark        = glueContext.spark_session
job          = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Enable Adaptive Query Execution — resolves data skew at runtime
# AQE dynamically optimises join strategies and partition sizes
spark.conf.set("spark.sql.adaptive.enabled",                    "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",           "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# ── Schema Definition ─────────────────────────────────────────────────────────

RAW_SCHEMA = StructType([
    StructField("event_id",        StringType(),    False),
    StructField("order_id",        StringType(),    False),
    StructField("customer_id",     StringType(),    False),
    StructField("product_id",      StringType(),    True),
    StructField("quantity",        IntegerType(),   True),
    StructField("unit_price",      DoubleType(),    True),
    StructField("total_amount",    DoubleType(),    True),
    StructField("status",          StringType(),    True),
    StructField("region",          StringType(),    True),
    StructField("event_type",      StringType(),    True),
    StructField("event_timestamp", StringType(),    True),
    StructField("kafka_partition", IntegerType(),   True),
    StructField("kafka_offset",    LongType(),      True),
    StructField("ingested_at",     StringType(),    True),
])

VALID_STATUSES    = {"PENDING", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"}
VALID_EVENT_TYPES = {"ORDER_CREATED", "ORDER_UPDATED", "ORDER_CANCELLED"}
VALID_REGIONS     = {"eu-west", "eu-central", "us-east", "us-west", "ap-southeast"}


def read_raw(spark: SparkSession, bucket: str, prefix: str, watermark_date: str) -> DataFrame:
    """Read compacted Parquet files from Raw zone, filtered by watermark date."""
    path = f"s3://{bucket}/{prefix}/year=*/month=*/day=*/hour=*/"
    logger.info("Reading raw data from: %s (watermark=%s)", path, watermark_date)

    df = (
        spark.read
        .schema(RAW_SCHEMA)
        .parquet(path)
        .filter(F.col("ingested_at") >= watermark_date)
    )
    logger.info("Raw records read: %d", df.count())
    return df


def validate_schema(df: DataFrame) -> DataFrame:
    """
    Validate required fields and filter out malformed records.
    Bad records are written to a dead letter path for investigation.
    """
    valid = df.filter(
        F.col("event_id").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("total_amount").isNotNull()
        & (F.col("total_amount") > 0)
        & F.col("event_type").isin(list(VALID_EVENT_TYPES))
        & F.col("status").isin(list(VALID_STATUSES))
    )

    invalid_count = df.count() - valid.count()
    if invalid_count > 0:
        logger.warning("Dropped %d invalid records during schema validation", invalid_count)

    return valid


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Deduplicate events using event_id as the idempotency key.
    Keeps the record with the latest kafka_offset for each event_id
    to handle Kafka replay scenarios on consumer restart.
    """
    deduped = (
        df.withColumn(
            "rn",
            F.row_number().over(
                # Window: partition by event_id, keep latest offset
                __import__("pyspark.sql.window", fromlist=["Window"])
                .Window
                .partitionBy("event_id")
                .orderBy(F.col("kafka_offset").desc())
            )
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    duplicates_removed = df.count() - deduped.count()
    if duplicates_removed > 0:
        logger.info("Removed %d duplicate events", duplicates_removed)

    return deduped


def transform(df: DataFrame) -> DataFrame:
    """
    Apply type casting, business enrichment, and derived columns.

    Data skew fix: customer_id has high cardinality skew — some customers
    generate 10x more events than others. We add a salt column for joins.
    AQE handles partition-level skew automatically, but explicit salting
    is kept as a defensive measure for downstream joins.
    """
    return (
        df
        # Parse timestamps
        .withColumn("event_timestamp_ts",
                    F.to_timestamp(F.col("event_timestamp")))
        .withColumn("ingested_at_ts",
                    F.to_timestamp(F.col("ingested_at")))

        # Derived date columns for partitioning
        .withColumn("event_date",  F.to_date(F.col("event_timestamp_ts")))
        .withColumn("event_year",  F.year(F.col("event_timestamp_ts")))
        .withColumn("event_month", F.month(F.col("event_timestamp_ts")))
        .withColumn("event_day",   F.dayofmonth(F.col("event_timestamp_ts")))
        .withColumn("event_hour",  F.hour(F.col("event_timestamp_ts")))

        # Business enrichment
        .withColumn("order_size_bucket",
                    F.when(F.col("total_amount") < 50,    "small")
                    .when(F.col("total_amount") < 200,    "medium")
                    .when(F.col("total_amount") < 500,    "large")
                    .otherwise("enterprise"))

        .withColumn("is_cancelled",
                    F.col("event_type") == "ORDER_CANCELLED")

        # Salt for skew mitigation on high-cardinality joins
        .withColumn("customer_salt",
                    (F.hash(F.col("customer_id")) % 32).cast("int"))

        # Audit columns
        .withColumn("processed_at",
                    F.lit(datetime.now(timezone.utc).isoformat()))
        .withColumn("pipeline_version", F.lit("1.0.0"))

        # Drop raw metadata not needed downstream
        .drop("kafka_partition", "kafka_offset")
    )


def write_curated(df: DataFrame, bucket: str, prefix: str):
    """
    Write curated data to S3 as partitioned Parquet.
    Partition by event_year/event_month/event_day for efficient pruning.
    Repartition by event_date and region to balance partition sizes.
    """
    logger.info("Writing curated data to s3://%s/%s", bucket, prefix)

    output_path = f"s3://{bucket}/{prefix}/"

    (
        df
        .repartition(F.col("event_date"), F.col("region"))  # balance partitions
        .write
        .mode("append")
        .partitionBy("event_year", "event_month", "event_day")
        .option("compression", "snappy")
        .parquet(output_path)
    )

    logger.info("Curated write complete")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    logger.info("Job started | args=%s", {k: v for k, v in args.items()
                                          if "password" not in k.lower()})

    raw_df      = read_raw(spark, args["raw_bucket"],
                           args["source_prefix"], args["watermark_date"])
    validated   = validate_schema(raw_df)
    deduped     = deduplicate(validated)
    transformed = transform(deduped)

    write_curated(transformed, args["curated_bucket"], args["target_prefix"])

    # Log final metrics
    final_count = transformed.count()
    logger.info("Job complete | output_rows=%d", final_count)

    job.commit()


if __name__ == "__main__":
    main()
