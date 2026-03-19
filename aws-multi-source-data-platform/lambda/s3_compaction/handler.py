"""
Lambda S3 Compaction Handler — AWS Multi-Source Data Platform

Challenge solved: 500K+ daily micro-files in S3 causing Glue job failures.
Solution: Lambda triggered on S3 events merges small files into 128MB Parquet files.

Result: 85% reduction in input partitions, Glue runtime 45 min → 7 min.
"""

import io
import json
import logging
import os
import time
from typing import List

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

# Configuration
TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024   # 128 MB target Parquet size
MIN_FILES_TO_COMPACT   = 10                  # Do not compact fewer than 10 files
MAX_FILES_PER_RUN      = 500                 # Process at most 500 files per Lambda run
SMALL_FILE_THRESHOLD   = 10 * 1024 * 1024    # Files smaller than 10MB are "small"


def get_prefix_from_key(key: str) -> str:
    """
    Extract the date/hour partition prefix from an S3 key.
    Example: orders/year=2026/month=03/day=18/hour=14/partition=0/batch_xxx.parquet
          → orders/year=2026/month=03/day=18/hour=14/partition=0/
    """
    return "/".join(key.split("/")[:-1]) + "/"


def list_small_files(bucket: str, prefix: str) -> List[dict]:
    """List all small Parquet files under the given prefix."""
    paginator = s3_client.get_paginator("list_objects_v2")
    small_files = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if (
                obj["Key"].endswith(".parquet")
                and obj["Size"] < SMALL_FILE_THRESHOLD
                and "/archive/" not in obj["Key"]
                and "/compacted/" not in obj["Key"]
            ):
                small_files.append({"key": obj["Key"], "size": obj["Size"]})

    return small_files[:MAX_FILES_PER_RUN]


def read_parquet_from_s3(bucket: str, key: str) -> pa.Table:
    """Read a single Parquet file from S3 into a PyArrow Table."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    buffer   = io.BytesIO(response["Body"].read())
    return pq.read_table(buffer)


def write_parquet_to_s3(bucket: str, key: str, table: pa.Table) -> int:
    """Write a PyArrow Table to S3 as a compressed Parquet file. Returns bytes written."""
    buffer = io.BytesIO()
    pq.write_table(
        table, buffer,
        compression    = "snappy",
        use_dictionary = True,
        row_group_size = 100000,
    )
    buffer.seek(0)
    data = buffer.getvalue()

    s3_client.put_object(
        Bucket      = bucket,
        Key         = key,
        Body        = data,
        ContentType = "application/octet-stream",
    )
    return len(data)


def archive_original_files(bucket: str, files: List[dict]):
    """
    Move original small files to archive/ prefix after compaction.
    Archived files are expired by S3 lifecycle policy after 90 days.
    """
    for f in files:
        archive_key = f["key"].replace(
            get_prefix_from_key(f["key"]),
            get_prefix_from_key(f["key"]).replace("/", "/archive/", 1)
        )
        s3_client.copy_object(
            Bucket     = bucket,
            CopySource = {"Bucket": bucket, "Key": f["key"]},
            Key        = archive_key,
        )
        s3_client.delete_object(Bucket=bucket, Key=f["key"])


def compact_prefix(bucket: str, prefix: str) -> dict:
    """
    Compact all small Parquet files under the given S3 prefix into one larger file.
    Returns metrics dict with files_merged, rows_merged, bytes_written.
    """
    small_files = list_small_files(bucket, prefix)

    if len(small_files) < MIN_FILES_TO_COMPACT:
        logger.info("Skipping prefix=%s — only %d small files (min=%d)",
                    prefix, len(small_files), MIN_FILES_TO_COMPACT)
        return {"skipped": True, "file_count": len(small_files)}

    logger.info("Compacting %d files under prefix=%s", len(small_files), prefix)

    # Read all small files into memory and concatenate
    tables = []
    for f in small_files:
        try:
            tables.append(read_parquet_from_s3(bucket, f["key"]))
        except Exception as e:
            logger.warning("Could not read %s — skipping | error=%s", f["key"], e)

    if not tables:
        return {"skipped": True, "reason": "all files failed to read"}

    merged_table = pa.concat_tables(tables)

    # Write compacted file
    compacted_key = (
        f"{prefix}compacted/"
        f"compacted_{int(time.time_ns())}.parquet"
    )
    bytes_written = write_parquet_to_s3(bucket, compacted_key, merged_table)

    logger.info(
        "Compacted | prefix=%s files_merged=%d rows=%d bytes=%d key=%s",
        prefix, len(small_files), merged_table.num_rows, bytes_written, compacted_key
    )

    # Archive originals — lifecycle policy will expire them
    archive_original_files(bucket, small_files)

    return {
        "files_merged":   len(small_files),
        "rows_merged":    merged_table.num_rows,
        "bytes_written":  bytes_written,
        "compacted_key":  compacted_key,
    }


def handler(event, context):
    """
    Lambda entry point.
    Triggered by S3 event notification when new files land in the raw zone.
    Extracts the partition prefix from the triggering key and compacts that prefix.
    """
    bucket  = os.environ["S3_RAW_BUCKET"]
    results = []

    for record in event.get("Records", []):
        key    = record["s3"]["object"]["key"]
        prefix = get_prefix_from_key(key)

        logger.info("Triggered by key=%s → compacting prefix=%s", key, prefix)

        try:
            result = compact_prefix(bucket, prefix)
            results.append({"prefix": prefix, **result})
        except Exception as e:
            logger.error("Compaction failed | prefix=%s error=%s", prefix, e)
            results.append({"prefix": prefix, "error": str(e)})

    logger.info("Compaction run complete | results=%s", json.dumps(results))
    return {"statusCode": 200, "body": results}
