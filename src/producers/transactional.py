"""Transactional producer for atomic read-process-write operations.

Demonstrates Kafka transactions enabling exactly-once semantics across
multiple topics: consume from one topic, process, produce to another,
and commit offsets atomically.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

import structlog
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer

from src.config import kafka_settings, producer_settings, transaction_settings
from src.models import OrderPlaced, PaymentProcessed, PaymentMethod

logger = structlog.get_logger(__name__)


class TransactionalPaymentProducer:
    """Transactional producer for exactly-once read-process-write.

    Implements the consume-transform-produce pattern:
    1. Begin transaction
    2. Consume order from orders.enriched
    3. Process payment (simulate)
    4. Produce payment result to payments.processed
    5. Commit consumer offsets within the transaction
    6. Commit transaction (atomic)

    If any step fails, the transaction is aborted and no side effects
    are visible to downstream consumers (isolation.level=read_committed).
    """

    INPUT_TOPIC = "orders.enriched"
    OUTPUT_TOPIC = "payments.processed"

    def __init__(self, instance_id: int = 0) -> None:
        self._transactional_id = f"{transaction_settings.transactional_id_prefix}-{instance_id}"

        producer_conf = {
            "bootstrap.servers": kafka_settings.bootstrap_servers,
            "client.id": f"payment-processor-{instance_id}",
            "transactional.id": self._transactional_id,
            "enable.idempotence": True,
            "acks": "all",
            "max.in.flight.requests.per.connection": 5,
            "transaction.timeout.ms": transaction_settings.timeout_ms,
            "compression.type": "lz4",
        }
        self._producer = Producer(producer_conf)

        consumer_conf = {
            "bootstrap.servers": kafka_settings.bootstrap_servers,
            "group.id": "payment-processor-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
        self._consumer = Consumer(consumer_conf)
        self._consumer.subscribe([self.INPUT_TOPIC])

        self._string_serializer = StringSerializer("utf_8")
        self._producer.init_transactions()
        logger.info("transactional_producer_initialized", transactional_id=self._transactional_id)

    def process_orders(self, max_messages: int = 100) -> int:
        processed = 0
        batch_size = 10

        while processed < max_messages:
            messages = self._consumer.consume(num_messages=batch_size, timeout=5.0)
            if not messages:
                break

            try:
                self._producer.begin_transaction()

                for msg in messages:
                    if msg.error():
                        continue
                    payment = self._process_payment(msg)
                    if payment is None:
                        continue
                    self._producer.produce(
                        topic=self.OUTPUT_TOPIC,
                        key=self._string_serializer(payment.order_id),
                        value=payment.model_dump_json().encode("utf-8"),
                        headers={
                            "transaction_id": self._transactional_id,
                            "source_partition": str(msg.partition()),
                            "source_offset": str(msg.offset()),
                        },
                    )
                    processed += 1

                offsets = self._consumer.position(
                    [TopicPartition(msg.topic(), msg.partition()) for msg in messages if not msg.error()]
                )
                self._producer.send_offsets_to_transaction(offsets, self._consumer.consumer_group_metadata())
                self._producer.commit_transaction()
                logger.info("transaction_committed", batch_size=len(messages))

            except KafkaException as e:
                logger.error("transaction_failed", error=str(e))
                self._producer.abort_transaction()

        return processed

    def _process_payment(self, msg: Any) -> Optional[PaymentProcessed]:
        import json
        import random
        try:
            order_data = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

        success = random.random() < 0.95
        failure_reason = None if success else "insufficient_funds"

        return PaymentProcessed(
            order_id=order_data.get("order_id", "unknown"),
            customer_id=order_data.get("customer_id", "unknown"),
            amount=Decimal(str(order_data.get("total_amount", 0))),
            currency=order_data.get("currency", "USD"),
            payment_method=PaymentMethod(order_data.get("payment_method", "credit_card")),
            status="success" if success else "failed",
            failure_reason=failure_reason,
        )

    def close(self) -> None:
        self._consumer.close()
        self._producer.flush(timeout=10.0)


def main() -> None:
    processor = TransactionalPaymentProducer(instance_id=0)
    try:
        processed = processor.process_orders(max_messages=1000)
        logger.info("processing_complete", total_processed=processed)
    finally:
        processor.close()


if __name__ == "__main__":
    main()
