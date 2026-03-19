"""
Kafka Event Producer — AWS Multi-Source Data Platform
Simulates real-time events from upstream source systems.
Implements idempotent production with Avro serialisation.
"""

import json
import time
import logging
import random
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Generator

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from config import KafkaConfig, SchemaRegistryConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class OrderEvent:
    event_id:       str
    order_id:       str
    customer_id:    str
    product_id:     str
    quantity:       int
    unit_price:     float
    total_amount:   float
    status:         str
    region:         str
    event_type:     str   # ORDER_CREATED | ORDER_UPDATED | ORDER_CANCELLED
    event_timestamp: str


ORDER_SCHEMA = """
{
  "type": "record",
  "name": "OrderEvent",
  "namespace": "com.dataplatform.orders",
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
"""


class OrderEventProducer:
    """
    Idempotent Kafka producer for order events.
    Uses Avro serialisation with Schema Registry for schema evolution safety.
    enable.idempotence=true guarantees exactly-once delivery per partition.
    """

    REGIONS   = ["eu-west", "eu-central", "us-east", "us-west", "ap-southeast"]
    STATUSES  = ["PENDING", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED"]
    EVENT_TYPES = ["ORDER_CREATED", "ORDER_UPDATED", "ORDER_CANCELLED"]

    def __init__(self, config: KafkaConfig, schema_config: SchemaRegistryConfig):
        self.topic = config.topic

        # Schema Registry for Avro serialisation
        schema_registry_client = SchemaRegistryClient({"url": schema_config.url})
        self.avro_serializer = AvroSerializer(
            schema_registry_client,
            ORDER_SCHEMA,
            lambda obj, ctx: asdict(obj)
        )

        # Idempotent producer — exactly-once per partition
        self.producer = Producer({
            "bootstrap.servers":        config.bootstrap_servers,
            "security.protocol":        "SASL_SSL",
            "sasl.mechanisms":          "AWS_MSK_IAM",
            "enable.idempotence":       True,       # exactly-once semantics
            "acks":                     "all",       # wait for all replicas
            "max.in.flight.requests.per.connection": 5,
            "retries":                  10,
            "retry.backoff.ms":         500,
            "compression.type":         "snappy",
            "batch.size":               65536,       # 64KB batches
            "linger.ms":                10,          # wait 10ms to fill batch
        })

        self._delivery_count = 0
        self._error_count    = 0

    def _delivery_callback(self, err, msg):
        if err:
            logger.error("Delivery failed | topic=%s partition=%d error=%s",
                         msg.topic(), msg.partition(), err)
            self._error_count += 1
        else:
            self._delivery_count += 1
            if self._delivery_count % 1000 == 0:
                logger.info("Delivered %d messages | errors=%d",
                            self._delivery_count, self._error_count)

    def _generate_event(self) -> OrderEvent:
        order_id    = f"ORD-{random.randint(100000, 999999)}"
        customer_id = f"CUST-{random.randint(1000, 9999)}"
        unit_price  = round(random.uniform(5.0, 500.0), 2)
        quantity    = random.randint(1, 20)

        return OrderEvent(
            event_id        = f"EVT-{int(time.time_ns())}",
            order_id        = order_id,
            customer_id     = customer_id,
            product_id      = f"PROD-{random.randint(100, 999)}",
            quantity        = quantity,
            unit_price      = unit_price,
            total_amount    = round(unit_price * quantity, 2),
            status          = random.choice(self.STATUSES),
            region          = random.choice(self.REGIONS),
            event_type      = random.choice(self.EVENT_TYPES),
            event_timestamp = datetime.now(timezone.utc).isoformat(),
        )

    def produce(self, events_per_second: int = 100, duration_seconds: int = None):
        """
        Produce events continuously at the specified rate.
        Uses customer_id as the partition key to ensure all events
        for the same customer land on the same partition — preserving order.
        """
        logger.info("Starting producer | rate=%d events/sec | topic=%s",
                    events_per_second, self.topic)

        start_time    = time.time()
        interval      = 1.0 / events_per_second
        events_sent   = 0

        try:
            while True:
                event = self._generate_event()

                self.producer.produce(
                    topic     = self.topic,
                    key       = event.customer_id,    # partition by customer
                    value     = self.avro_serializer(
                        event,
                        SerializationContext(self.topic, MessageField.VALUE)
                    ),
                    on_delivery = self._delivery_callback,
                )

                events_sent += 1
                self.producer.poll(0)    # non-blocking poll for callbacks

                # Rate control
                time.sleep(interval)

                if duration_seconds and (time.time() - start_time) >= duration_seconds:
                    break

        except KeyboardInterrupt:
            logger.info("Producer interrupted by user")
        finally:
            self.producer.flush(timeout=30)
            logger.info("Producer finished | sent=%d errors=%d",
                        events_sent, self._error_count)


if __name__ == "__main__":
    from config import KafkaConfig, SchemaRegistryConfig

    producer = OrderEventProducer(
        config        = KafkaConfig(),
        schema_config = SchemaRegistryConfig(),
    )
    producer.produce(events_per_second=500)
