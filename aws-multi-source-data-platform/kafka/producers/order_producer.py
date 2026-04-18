"""
Kafka Event Producer — Banking Data Platform
Simulates real-time order events from upstream systems (e.g., e-commerce, core banking).
Features:
  - Avro serialization with Schema Registry (schema evolution safety)
  - Idempotent producer with exactly-once semantics per partition
  - Customer-based partitioning (preserves per-customer event order)
  - Robust error handling, monitoring, and graceful shutdown
  - Configurable rate limiting and duration
"""

import time
import logging
import random
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from config import KafkaConfig, SchemaRegistryConfig

# =============================================================================
# Logging Configuration
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class OrderEvent:
    """Order event model - immutable for thread safety and clarity"""
    event_id: str
    order_id: str
    customer_id: str
    product_id: str
    quantity: int
    unit_price: float
    total_amount: float
    status: str
    region: str
    event_type: str          # ORDER_CREATED | ORDER_UPDATED | ORDER_CANCELLED
    event_timestamp: str



# Avro Schema (kept close to the model for maintainability)
ORDER_AVRO_SCHEMA = """
{
  "type": "record",
  "name": "OrderEvent",
  "namespace": "com.bank.dataplatform.orders",
  "doc": "Real-time order event for banking data lake ingestion",
  "fields": [
    {"name": "event_id",        "type": "string"},
    {"name": "order_id",        "type": "string"},
    {"name": "customer_id",     "type": "string"},
    {"name": "product_id",      "type": "string"},
    {"name": "quantity",        "type": "int"},
    {"name": "unit_price",      "type": "double"},
    {"name": "total_amount",    "type": "double"},
    {"name": "status",          "type": "string"},
    {"name": "region",          "type": "string"},
    {"name": "event_type",      "type": "string"},
    {"name": "event_timestamp", "type": "string"}
  ]
}


class OrderEventProducer:
    """
    High-performance, idempotent Kafka producer for order events.
    Designed for banking workloads with strong consistency and observability.
    """

    REGIONS = ["eu-west-1", "eu-central-1", "us-east-1", "us-west-2", "ap-southeast-1"]
    STATUSES = ["PENDING", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
    EVENT_TYPES = ["ORDER_CREATED", "ORDER_UPDATED", "ORDER_CANCELLED"]

    def __init__(self, config: KafkaConfig, schema_config: SchemaRegistryConfig):
        self.topic = config.topic

        # Schema Registry + Avro Serializer
        schema_registry_client = SchemaRegistryClient({"url": schema_config.url})
        self.avro_serializer = AvroSerializer(
            schema_registry_client,
            ORDER_AVRO_SCHEMA,
            lambda obj, ctx: asdict(obj)
        )

        # Idempotent Producer Configuration (Exactly-once semantics)
        producer_config = {
            "bootstrap.servers": config.bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "AWS_MSK_IAM",
            "enable.idempotence": True,               # Critical for banking
            "acks": "all",                            # Strong durability
            "max.in.flight.requests.per.connection": 5,
            "retries": 10,
            "retry.backoff.ms": 500,
            "compression.type": "snappy",
            "batch.size": 65536,                      # 64 KB
            "linger.ms": 10,                          # Small batch delay
            "delivery.timeout.ms": 30000,             # 30s total delivery window
            "socket.timeout.ms": 10000,
        }

        self.producer = Producer(producer_config)

        # Monitoring counters
        self._delivered = 0
        self._errors = 0

    def _delivery_callback(self, err, msg):
        """Callback executed when broker acknowledges (or fails) the message"""
        if err is not None:
            logger.error("Delivery failed | topic=%s | partition=%s | error=%s",
                         msg.topic(), msg.partition(), err)
            self._errors += 1
        else:
            self._delivered += 1
            if self._delivered % 1000 == 0:
                logger.info("Delivered %d messages | errors=%d | throughput=%.1f msg/s",
                            self._delivered, self._errors,
                            self._delivered / (time.time() - self._start_time))

    def _generate_event(self) -> OrderEvent:
        """Generate realistic synthetic order event"""
        order_id = f"ORD-{random.randint(100_000, 999_999)}"
        customer_id = f"CUST-{random.randint(10_000, 99_999)}"
        product_id = f"PROD-{random.randint(1_000, 9_999)}"

        unit_price = round(random.uniform(9.99, 999.99), 2)
        quantity = random.randint(1, 25)

        return OrderEvent(
            event_id=f"EVT-{int(time.time_ns() / 1000)}",   # microsecond precision
            order_id=order_id,
            customer_id=customer_id,
            product_id=product_id,
            quantity=quantity,
            unit_price=unit_price,
            total_amount=round(unit_price * quantity, 2),
            status=random.choice(self.STATUSES),
            region=random.choice(self.REGIONS),
            event_type=random.choice(self.EVENT_TYPES),
            event_timestamp=datetime.now(timezone.utc).isoformat()
        )

    def produce(self,
                events_per_second: int = 100,
                duration_seconds: Optional[int] = None):
        """
        Produce events at controlled rate with graceful shutdown.
        Uses customer_id as key → guarantees per-customer ordering.
        """
        logger.info("Starting OrderEventProducer | rate=%d events/sec | topic=%s",
                    events_per_second, self.topic)

        self._start_time = time.time()
        interval = 1.0 / events_per_second
        events_sent = 0

        try:
            while True:
                event = self._generate_event()

                self.producer.produce(
                    topic=self.topic,
                    key=event.customer_id.encode("utf-8"),   # Partition by customer
                    value=self.avro_serializer(
                        event,
                        SerializationContext(self.topic, MessageField.VALUE)
                    ),
                    on_delivery=self._delivery_callback,
                )

                events_sent += 1
                self.producer.poll(0)   # Trigger callbacks

                # Precise rate control
                time.sleep(interval)

                if duration_seconds and (time.time() - self._start_time) >= duration_seconds:
                    break

        except KeyboardInterrupt:
            logger.info("Producer stopped by user (Ctrl+C)")
        except Exception as e:
            logger.exception("Unexpected error in producer: %s", e)
        finally:
            logger.info("Flushing remaining messages...")
            self.producer.flush(timeout=60)
            duration = time.time() - self._start_time
            throughput = events_sent / duration if duration > 0 else 0

            logger.info("Producer shutdown complete | "
                        "sent=%d | errors=%d | duration=%.1fs | throughput=%.1f msg/s",
                        events_sent, self._errors, duration, throughput)


if __name__ == "__main__":
    from config import KafkaConfig, SchemaRegistryConfig

    producer = OrderEventProducer(
        config=KafkaConfig(),
        schema_config=SchemaRegistryConfig()
    )

    # Run at 500 events/sec indefinitely (or Ctrl+C to stop)
    producer.produce(events_per_second=500)
