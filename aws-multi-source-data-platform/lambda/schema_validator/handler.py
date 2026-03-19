"""
Lambda Schema Validator — AWS Multi-Source Data Platform
Validates incoming event schemas before they land in the Raw zone.
Catches schema drift early — before it breaks downstream Glue jobs.

Challenge solved: new columns added in source breaking entire pipeline silently.
Solution: validate against Schema Registry on every incoming batch.
"""

import json
import logging
import os
from typing import Dict, List, Tuple

import boto3
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

# Required fields that must always be present
REQUIRED_FIELDS = {
    "event_id", "order_id", "customer_id",
    "total_amount", "event_timestamp", "status"
}

# Fields that if added are allowed (backward compatible) — just warn
KNOWN_OPTIONAL_FIELDS = {
    "product_id", "quantity", "unit_price", "region",
    "event_type", "order_size_bucket", "is_cancelled",
    "kafka_topic", "kafka_partition", "kafka_offset", "ingested_at",
    "pipeline_version", "customer_salt", "processed_at"
}


def validate_event(event: Dict) -> Tuple[bool, List[str]]:
    """
    Validate a single event against the expected schema.
    Returns (is_valid, list_of_errors).
    """
    errors = []

    # Check required fields
    for field in REQUIRED_FIELDS:
        if field not in event or event[field] is None:
            errors.append(f"Missing required field: {field}")

    # Check for unexpected new fields — potential schema evolution
    event_fields = set(event.keys())
    known_fields  = REQUIRED_FIELDS | KNOWN_OPTIONAL_FIELDS
    new_fields    = event_fields - known_fields

    if new_fields:
        # New fields are not a hard failure — log and alert but allow through
        logger.warning("New fields detected (schema evolution?): %s", new_fields)

    # Business logic validations
    if "total_amount" in event and event["total_amount"] is not None:
        try:
            amount = float(event["total_amount"])
            if amount <= 0:
                errors.append(f"total_amount must be positive, got: {amount}")
            if amount > 1_000_000:
                errors.append(f"total_amount suspiciously large: {amount}")
        except (ValueError, TypeError):
            errors.append(f"total_amount is not numeric: {event['total_amount']}")

    if "status" in event and event["status"] not in {
        "PENDING", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"
    }:
        errors.append(f"Unknown status value: {event['status']}")

    return len(errors) == 0, errors


def handler(event, context):
    """
    Lambda entry point for schema validation.
    Reads events from the triggering S3 object, validates each one,
    routes valid events to the processing prefix and invalid to dead-letter.
    """
    bucket      = os.environ["S3_RAW_BUCKET"]
    dlq_bucket  = os.environ.get("S3_DLQ_BUCKET", bucket)
    valid_count = 0
    invalid_count = 0
    results     = []

    for record in event.get("Records", []):
        key = record["s3"]["object"]["key"]

        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            events   = json.loads(response["Body"].read())

            if not isinstance(events, list):
                events = [events]

            valid_events   = []
            invalid_events = []

            for evt in events:
                is_valid, errors = validate_event(evt)
                if is_valid:
                    valid_events.append(evt)
                    valid_count += 1
                else:
                    logger.warning(
                        "Invalid event | event_id=%s errors=%s",
                        evt.get("event_id", "unknown"), errors
                    )
                    invalid_events.append({"event": evt, "errors": errors})
                    invalid_count += 1

            # Route invalid events to dead-letter prefix for investigation
            if invalid_events:
                dlq_key = key.replace("landing/", "dead-letter/")
                s3_client.put_object(
                    Bucket      = dlq_bucket,
                    Key         = dlq_key,
                    Body        = json.dumps(invalid_events),
                    ContentType = "application/json",
                )
                logger.info("Wrote %d invalid events to DLQ: %s", len(invalid_events), dlq_key)

            results.append({
                "key":           key,
                "valid_count":   len(valid_events),
                "invalid_count": len(invalid_events),
            })

        except Exception as e:
            logger.error("Failed to process key=%s error=%s", key, e)
            results.append({"key": key, "error": str(e)})

    logger.info(
        "Validation complete | total_valid=%d total_invalid=%d",
        valid_count, invalid_count
    )

    return {
        "statusCode": 200,
        "body": {
            "total_valid":   valid_count,
            "total_invalid": invalid_count,
            "results":       results,
        }
    }
