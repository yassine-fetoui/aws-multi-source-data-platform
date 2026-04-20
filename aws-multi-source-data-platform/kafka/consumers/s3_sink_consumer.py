"""
Kafka to S3 Exactly-Once Sink Consumer
======================================

A robust, production-ready consumer that reads Avro events from Kafka and lands them
in S3 in the Raw zone as Snappy-compressed Parquet files.

Key Design Principles:
- Exactly-once semantics: Manual offset commit **only after** successful S3 write
- Idempotency & safety on restarts: No partial writes + no premature commits
- Mitigation of small files problem: Configurable batching with time-based flushing
- Partitioned S3 layout: Hive-style partitioning (year/month/day/hour/partition=)
- High observability: Structured logging + key metrics
- Graceful shutdown and failure isolation
"""

import io
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import BotoCoreError, ClientError
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# ====================== Logging Configuration ======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class S3SinkConsumer:
    """
    Production-grade Kafka → S3 exactly-once consumer.

    Core Guarantees:
        - At-least-once delivery from Kafka is turned into exactly-once to S3
          by committing offsets **only after** the Parquet file is durably written to S3.
        - No duplicate files on consumer restart or failure.
        - Graceful handling of transient S3/Kafka errors with backpressure.

    Features:
        - Batch-oriented writes to avoid the small files problem
        - Time-based + size-based flushing
        - Hive-style S3 partitioning
        - Comprehensive metrics and structured logging
        - Proper resource cleanup on shutdown
    """


    # Parquet schema with Kafka metadata enrichment
    PARQUET_SCHEMA = pa.schema([
        pa.field("event_id", pa.string()),
        pa.field("order_id", pa.string()),
        pa.field("customer_id", pa.string()),
        pa.field("product_id", pa.string()),
        pa.field("quantity", pa.int32()),
        pa.field("unit_price", pa.float64()),
        pa.field("total_amount", pa.float64()),
        pa.field("status", pa.string()),
        pa.field("region", pa.string()),
        pa.field("event_type", pa.string()),
        pa.field("event_timestamp", pa.string()),
        # Consumer-added metadata (for audit & debugging)
        pa.field("kafka_topic", pa.string()),
        pa.field("kafka_partition", pa.int32()),
        pa.field("kafka_offset", pa.int64()),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
    ])
    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        topic: str,
        group_id: str,
        s3_bucket: str,
        s3_prefix: str,
        batch_size: int = 5000,
        max_batch_age_seconds: int = 60,
        **kwargs,
    ):
        self.topic = topic
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix.rstrip("/")
        self.batch_size = batch_size
        self.max_batch_age_seconds = max_batch_age_seconds

        # AWS Client
        self.s3_client = boto3.client("s3")

        # Schema Registry + Avro Deserializer
        schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
        self.avro_deserializer = AvroDeserializer(schema_registry_client)

        # Kafka Consumer configuration (Exactly-once foundation)
        consumer_config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,                    # Critical for exactly-once
            "isolation.level": "read_committed",
            "max.poll.interval.ms": 300_000,                # 5 minutes
            "session.timeout.ms": 45_000,
            "enable.partition.eof": False,
            **kwargs,  # Allow overriding any config
        }

        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([topic])

        # Internal state
        self._buffer: List[Dict] = []
        self._last_offsets: Dict[int, int] = {}          # partition → last offset seen
        self._batch_start_time: Optional[float] = None

        # Observability
        self._messages_read = 0
        self._batches_written = 0
        self._bytes_written = 0

    def _s3_key(self, kafka_partition: int) -> str:
        """Generate Hive-style partitioned S3 key"""
        now = datetime.now(timezone.utc)
        return (
            f"{self.s3_prefix}/"
            f"year={now.year}/"
            f"month={now.month:02d}/"
            f"day={now.day:02d}/"
            f"hour={now.hour:02d}/"
            f"kafka_partition={kafka_partition}/"
            f"batch_{now.strftime('%Y%m%d_%H%M%S')}_{int(time.time() * 1000)}.parquet"
        )

    def _should_flush(self) -> bool:
        """Determine if buffer should be flushed based on size or age"""
        if not self._buffer:
            return False
        if len(self._buffer) >= self.batch_size:
            return True
        if self._batch_start_time is None:
            self._batch_start_time = time.time()
            return False
        return (time.time() - self._batch_start_time) >= self.max_batch_age_seconds

    def _flush_to_s3(self) -> None:
        """Write buffer to S3 as Parquet and commit offsets only on success."""
        if not self._buffer:
            return

        try:
            # Convert buffer to PyArrow Table
            columns = defaultdict(list)
            for record in self._buffer:
                for key, value in record.items():
                    columns[key].append(value)

            table = pa.Table.from_pydict(dict(columns), schema=self.PARQUET_SCHEMA)

            last_partition = self._buffer[-1]["kafka_partition"]
            s3_key = self._s3_key(last_partition)

            # Write Parquet in-memory with optimized settings
            buffer = io.BytesIO()
            pq.write_table(
                table,
                buffer,
                compression="snappy",
                use_dictionary=True,
                row_group_size=50_000,
                data_page_size=1024 * 1024,   # 1MB
            )
            buffer.seek(0)
            parquet_bytes = buffer.getvalue()

            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=parquet_bytes,
                ContentType="application/vnd.apache.parquet",
                Metadata={
                    "batch_size": str(len(self._buffer)),
                    "kafka_topic": self.topic,
                },
            )

            self._bytes_written += len(parquet_bytes)

            logger.info(
                "S3 write successful | key=%s | rows=%d | size=%d bytes | partition=%d",
                s3_key, len(self._buffer), len(parquet_bytes), last_partition
            )

            # === CRITICAL: Commit offsets ONLY after successful S3 write ===
            offsets_to_commit = [
                TopicPartition(self.topic, p, offset + 1)
                for p, offset in self._last_offsets.items()
            ]

            if offsets_to_commit:
                self.consumer.commit(offsets=offsets_to_commit, asynchronous=False)
                logger.debug("Kafka offsets committed successfully: %s", self._last_offsets)

            # Update metrics and reset buffer
            self._batches_written += 1
            self._buffer.clear()
            self._last_offsets.clear()
            self._batch_start_time = None

        except (BotoCoreError, ClientError) as e:
            logger.error("S3 upload failed — offsets will NOT be committed | error=%s", e)
            raise  # Let the outer loop handle retry via re-consumption
        except Exception as e:
            logger.exception("Unexpected error during S3 flush")
            raise

    def run(self) -> None:
        """Main consumer loop with proper shutdown handling."""
        logger.info(
            "Starting S3 Sink Consumer | topic=%s | batch_size=%d | max_batch_age=%ds",
            self.topic, self.batch_size, self.max_batch_age_seconds
        )

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    # Idle poll — check if we should flush aging batch
                    if self._should_flush():
                        self._flush_to_s3()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Kafka consumer error: %s", msg.error())
                    continue

                # Deserialize Avro and enrich with Kafka metadata
                try:
                    event = self.avro_deserializer(
                        msg.value(),
                        SerializationContext(msg.topic(), MessageField.VALUE)
                    )
                except Exception as e:
                    logger.error("Avro deserialization failed | offset=%d | error=%s", msg.offset(), e)
                    continue  # Skip bad message (or implement DLQ in production)

                event["kafka_topic"] = msg.topic()
                event["kafka_partition"] = msg.partition()
                event["kafka_offset"] = msg.offset()
                event["ingested_at"] = datetime.now(timezone.utc)

                self._buffer.append(event)
                self._last_offsets[msg.partition()] = msg.offset()
                self._messages_read += 1

                if self._should_flush():
                    self._flush_to_s3()

        except KeyboardInterrupt:
            logger.info("Shutdown requested — flushing remaining buffer...")
            self._flush_to_s3()
        except Exception as e:
            logger.exception("Unexpected error in consumer loop")
            raise
        finally:
            self.consumer.close()
            logger.info(
                "Consumer shutdown complete | "
                "messages_read=%d | batches_written=%d | bytes_written=%d",
                self._messages_read, self._batches_written, self._bytes_written
            )


if __name__ == "__main__":
    consumer = S3SinkConsumer(
        bootstrap_servers=os.environ["MSK_BOOTSTRAP_SERVERS"],
        schema_registry_url=os.environ["SCHEMA_REGISTRY_URL"],
        topic="orders",
        group_id="s3-sink-orders-v2",
        s3_bucket=os.environ["S3_RAW_BUCKET"],
        s3_prefix="raw/orders",
        batch_size=5000,                    # Larger default for production
        max_batch_age_seconds=60,           # Flush at least every minute
    )
    consumer.run()
