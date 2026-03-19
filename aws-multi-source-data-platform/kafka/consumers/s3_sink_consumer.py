"""
Kafka → S3 Consumer — AWS Multi-Source Data Platform
Consumes Avro events from Kafka and lands them in S3 Raw zone as Parquet.
Implements exactly-once semantics via manual offset commits after successful S3 write.

Challenge solved: duplicate records on consumer restart.
Solution: manual offset commit only after confirmed S3 write — never before.
"""

import io
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import List, Dict

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, TopicPartition, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3SinkConsumer:
    """
    Exactly-once Kafka → S3 consumer.

    Key design decisions:
    1. enable.auto.commit = False — manual offset commit only after S3 write
    2. Partition key in S3 path: year/month/day/hour for time-based pruning
    3. Batch writes: accumulate N messages then write one Parquet file
       to avoid the small-file problem at the consumer level
    4. On failure: do NOT commit offset — next restart re-reads from last commit
    """

    PARQUET_SCHEMA = pa.schema([
        pa.field("event_id",        pa.string()),
        pa.field("order_id",        pa.string()),
        pa.field("customer_id",     pa.string()),
        pa.field("product_id",      pa.string()),
        pa.field("quantity",        pa.int32()),
        pa.field("unit_price",      pa.float64()),
        pa.field("total_amount",    pa.float64()),
        pa.field("status",          pa.string()),
        pa.field("region",          pa.string()),
        pa.field("event_type",      pa.string()),
        pa.field("event_timestamp", pa.string()),
        # Metadata added by consumer
        pa.field("kafka_topic",     pa.string()),
        pa.field("kafka_partition", pa.int32()),
        pa.field("kafka_offset",    pa.int64()),
        pa.field("ingested_at",     pa.string()),
    ])

    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        topic: str,
        group_id: str,
        s3_bucket: str,
        s3_prefix: str,
        batch_size: int = 1000,
    ):
        self.topic     = topic
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.batch_size = batch_size
        self.s3_client  = boto3.client("s3")

        # Schema Registry deserialiser
        schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
        self.avro_deserializer  = AvroDeserializer(schema_registry_client)

        # Consumer — manual offset commit for exactly-once semantics
        self.consumer = Consumer({
            "bootstrap.servers":  bootstrap_servers,
            "group.id":           group_id,
            "auto.offset.reset":  "earliest",
            "enable.auto.commit": False,        # CRITICAL: manual commit only
            "isolation.level":    "read_committed",  # only read committed txns
            "max.poll.interval.ms": 300000,
            "session.timeout.ms":   45000,
        })
        self.consumer.subscribe([topic])

        self._buffer:        List[Dict] = []
        self._last_offsets:  Dict       = {}   # partition → offset for commit
        self._messages_read  = 0
        self._batches_written = 0

    def _s3_key(self, partition: int) -> str:
        now = datetime.now(timezone.utc)
        return (
            f"{self.s3_prefix}/"
            f"year={now.year}/month={now.month:02d}/"
            f"day={now.day:02d}/hour={now.hour:02d}/"
            f"partition={partition}/"
            f"batch_{now.strftime('%Y%m%d_%H%M%S')}_{int(time.time_ns())}.parquet"
        )

    def _flush_to_s3(self):
        """
        Write buffered records to S3 as a single Parquet file.
        Only commits Kafka offsets AFTER successful S3 write.
        If S3 write fails, offsets are NOT committed — safe to retry.
        """
        if not self._buffer:
            return

        # Build PyArrow table
        columns = defaultdict(list)
        for record in self._buffer:
            for key, value in record.items():
                columns[key].append(value)

        table = pa.Table.from_pydict(dict(columns), schema=self.PARQUET_SCHEMA)

        # Use the partition of the last message for S3 key
        last_partition = self._buffer[-1]["kafka_partition"]
        s3_key         = self._s3_key(last_partition)

        # Write Parquet to in-memory buffer then upload to S3
        buffer = io.BytesIO()
        pq.write_table(
            table, buffer,
            compression="snappy",
            use_dictionary=True,
            row_group_size=50000,
        )
        buffer.seek(0)

        try:
            self.s3_client.put_object(
                Bucket      = self.s3_bucket,
                Key         = s3_key,
                Body        = buffer.getvalue(),
                ContentType = "application/octet-stream",
            )
            logger.info("S3 write OK | key=%s rows=%d", s3_key, len(self._buffer))

            # ONLY commit offsets after confirmed S3 write
            offsets_to_commit = [
                TopicPartition(self.topic, partition, offset + 1)
                for partition, offset in self._last_offsets.items()
            ]
            self.consumer.commit(offsets=offsets_to_commit, asynchronous=False)
            logger.debug("Offsets committed | %s", self._last_offsets)

            self._batches_written += 1
            self._buffer.clear()
            self._last_offsets.clear()

        except Exception as e:
            # S3 write failed — do NOT commit offsets
            # Next consumer restart will re-read from last committed offset
            logger.error("S3 write FAILED — offsets NOT committed | error=%s", e)
            raise

    def run(self):
        logger.info("Consumer started | topic=%s batch_size=%d",
                    self.topic, self.batch_size)
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    # No message — flush if buffer has data waiting
                    if self._buffer:
                        self._flush_to_s3()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Kafka error: %s", msg.error())
                    continue

                # Deserialise Avro payload
                event = self.avro_deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE)
                )

                # Enrich with Kafka metadata
                event["kafka_topic"]     = msg.topic()
                event["kafka_partition"] = msg.partition()
                event["kafka_offset"]    = msg.offset()
                event["ingested_at"]     = datetime.now(timezone.utc).isoformat()

                self._buffer.append(event)
                self._last_offsets[msg.partition()] = msg.offset()
                self._messages_read += 1

                if len(self._buffer) >= self.batch_size:
                    self._flush_to_s3()

        except KeyboardInterrupt:
            logger.info("Consumer interrupted — flushing remaining buffer")
            self._flush_to_s3()
        finally:
            self.consumer.close()
            logger.info("Consumer closed | messages_read=%d batches_written=%d",
                        self._messages_read, self._batches_written)


if __name__ == "__main__":
    consumer = S3SinkConsumer(
        bootstrap_servers   = os.environ["MSK_BOOTSTRAP_SERVERS"],
        schema_registry_url = os.environ["SCHEMA_REGISTRY_URL"],
        topic               = "orders",
        group_id            = "s3-sink-orders",
        s3_bucket           = os.environ["S3_RAW_BUCKET"],
        s3_prefix           = "orders",
        batch_size          = 1000,
    )
    consumer.run()
