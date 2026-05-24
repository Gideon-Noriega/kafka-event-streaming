"""Idempotent producer with exactly-once delivery guarantees."""

from __future__ import annotations

import signal
import sys
import time
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

import structlog
from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from prometheus_client import Counter, Histogram, start_http_server

from src.config import kafka_settings, producer_settings, app_settings
from src.models import OrderItem, OrderPlaced, PaymentMethod, Region
from src.producers.partitioner import RegionAwarePartitioner

logger = structlog.get_logger(__name__)

MESSAGES_PRODUCED = Counter("kafka_messages_produced_total", "Total messages produced", ["topic", "status"])
PRODUCE_LATENCY = Histogram("kafka_produce_latency_seconds", "Produce latency", ["topic"])
PRODUCE_ERRORS = Counter("kafka_produce_errors_total", "Produce errors", ["topic", "error_type"])


def order_to_dict(order: OrderPlaced, ctx: SerializationContext) -> dict[str, Any]:
    return {
        "order_id": order.order_id,
        "customer_id": order.customer_id,
        "items": [{"product_id": i.product_id, "product_name": i.product_name,
                   "quantity": i.quantity, "unit_price": float(i.unit_price)} for i in order.items],
        "total_amount": float(order.total_amount),
        "currency": order.currency,
        "payment_method": order.payment_method.value,
        "shipping_address": order.shipping_address,
        "region": order.region.value,
        "placed_at": int(order.placed_at.timestamp() * 1000),
    }


class IdempotentOrderProducer:
    """Idempotent Kafka producer with exactly-once per-partition delivery.

    Configuration rationale:
    - enable.idempotence=true: PID+sequence dedup at broker
    - acks=all: Full ISR acknowledgment before success
    - max.in.flight=5: Maximum allowed with idempotence
    - linger.ms=20: Trade 20ms latency for batching throughput
    - compression=lz4: Fast compression, ~60% ratio on JSON-like data
    """

    TOPIC = "orders.placed"

    def __init__(self) -> None:
        self._partitioner = RegionAwarePartitioner(num_partitions=12)
        schema_registry_conf = {"url": kafka_settings.schema_registry_url}
        self._schema_registry = SchemaRegistryClient(schema_registry_conf)

        with open("src/schemas/order_placed_v1.avsc") as f:
            schema_str = f.read()

        self._avro_serializer = AvroSerializer(
            schema_registry_client=self._schema_registry,
            schema_str=schema_str, to_dict=order_to_dict,
        )
        self._string_serializer = StringSerializer("utf_8")

        self._producer = Producer({
            "bootstrap.servers": kafka_settings.bootstrap_servers,
            "client.id": producer_settings.client_id,
            "enable.idempotence": True,
            "acks": producer_settings.acks,
            "max.in.flight.requests.per.connection": producer_settings.max_in_flight,
            "retries": producer_settings.retries,
            "retry.backoff.ms": producer_settings.retry_backoff_ms,
            "compression.type": producer_settings.compression,
            "linger.ms": producer_settings.linger_ms,
            "batch.size": producer_settings.batch_size,
            "queue.buffering.max.messages": 100000,
            "delivery.timeout.ms": 120000,
        })
        logger.info("idempotent_producer_initialized")

    def _delivery_callback(self, err: Optional[KafkaError], msg: Any) -> None:
        if err is not None:
            MESSAGES_PRODUCED.labels(topic=self.TOPIC, status="error").inc()
            PRODUCE_ERRORS.labels(topic=self.TOPIC, error_type=err.name()).inc()
            logger.error("delivery_failed", error=err.str(), partition=msg.partition())
        else:
            MESSAGES_PRODUCED.labels(topic=self.TOPIC, status="success").inc()
            logger.debug("delivered", partition=msg.partition(), offset=msg.offset())

    def produce_order(self, order: OrderPlaced) -> None:
        start_time = time.time()
        try:
            key_bytes = order.customer_id.encode("utf-8")
            partition = self._partitioner(
                key=key_bytes, all_partitions=list(range(12)),
                available_partitions=list(range(12)), region=order.region,
            )
            self._producer.produce(
                topic=self.TOPIC, partition=partition,
                key=self._string_serializer(order.customer_id),
                value=self._avro_serializer(order, SerializationContext(self.TOPIC, MessageField.VALUE)),
                on_delivery=self._delivery_callback,
                headers={"correlation_id": order.order_id, "source": "order-service", "region": order.region.value},
            )
            self._producer.poll(0)
            PRODUCE_LATENCY.labels(topic=self.TOPIC).observe(time.time() - start_time)
        except BufferError:
            logger.warning("buffer_full", order_id=order.order_id)
            self._producer.flush(timeout=5.0)
            self.produce_order(order)
        except KafkaException as e:
            PRODUCE_ERRORS.labels(topic=self.TOPIC, error_type="kafka_exception").inc()
            raise

    def produce_batch(self, orders: list[OrderPlaced]) -> int:
        queued = 0
        for order in orders:
            try:
                self.produce_order(order)
                queued += 1
            except Exception:
                pass
        self._producer.flush(timeout=30.0)
        logger.info("batch_produced", total=len(orders), queued=queued)
        return queued

    def close(self) -> None:
        self._producer.flush(timeout=30.0)
        logger.info("producer_shutdown_complete")


def generate_sample_orders(count: int = 10) -> list[OrderPlaced]:
    import random
    products = [
        ("LAPTOP-001", "MacBook Pro 16in", Decimal("2499.99")),
        ("PHONE-001", "iPhone 15 Pro", Decimal("1199.99")),
        ("HEADPHONES-001", "AirPods Pro", Decimal("249.99")),
        ("TABLET-001", "iPad Air", Decimal("599.99")),
        ("WATCH-001", "Apple Watch Ultra", Decimal("799.99")),
        ("CHARGER-001", "MagSafe Charger", Decimal("39.99")),
    ]
    orders = []
    for _ in range(count):
        selected = random.sample(products, random.randint(1, 3))
        items = [OrderItem(product_id=p[0], product_name=p[1], quantity=random.randint(1, 2), unit_price=p[2]) for p in selected]
        total = sum(i.total_price for i in items)
        orders.append(OrderPlaced(
            customer_id=f"cust-{uuid.uuid4().hex[:8]}", items=items, total_amount=total,
            payment_method=random.choice(list(PaymentMethod)),
            shipping_address="123 Main St, New York, NY 10001",
            region=random.choice(list(Region)),
        ))
    return orders


def main() -> None:
    start_http_server(app_settings.metrics_port)
    producer = IdempotentOrderProducer()

    def shutdown(signum: int, frame: Any) -> None:
        producer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    orders = generate_sample_orders(count=50)
    logger.info("producing_orders", count=len(orders))
    producer.produce_batch(orders)
    producer.close()


if __name__ == "__main__":
    main()
