"""Exactly-once consumer using transactional consume-transform-produce.

Demonstrates the EOS (Exactly-Once Semantics) pattern:
- Consumer reads with isolation.level=read_committed
- Processing happens within a transaction boundary
- Output + offset commit are atomic via send_offsets_to_transaction
"""

from __future__ import annotations

import json
import signal
import sys
import time
from typing import Any, Optional

import structlog
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from prometheus_client import Counter, start_http_server

from src.config import kafka_settings, consumer_settings, transaction_settings, app_settings

logger = structlog.get_logger(__name__)

EOS_PROCESSED = Counter("kafka_eos_messages_processed_total", "EOS processed messages")
EOS_TRANSACTIONS = Counter("kafka_eos_transactions_total", "EOS transactions", ["status"])


class ExactlyOnceProcessor:
    """Exactly-once consume-transform-produce processor.

    Architecture:
    1. Consumer polls messages (isolation.level=read_committed)
    2. Begin producer transaction
    3. Transform and produce output messages
    4. Send consumer offsets to transaction (atomic with output)
    5. Commit transaction

    Failure modes:
    - If transaction fails: abort, consumer re-reads from last committed offset
    - If consumer crashes: new instance starts from last committed offset
    - No duplicates because offsets and outputs are atomic
    """

    INPUT_TOPIC = "orders.placed"
    OUTPUT_TOPIC = "orders.enriched"

    def __init__(self, instance_id: int = 0) -> None:
        self._instance_id = instance_id
        self._running = False

        self._producer = Producer({
            "bootstrap.servers": kafka_settings.bootstrap_servers,
            "transactional.id": f"eos-processor-{instance_id}",
            "enable.idempotence": True,
            "acks": "all",
            "max.in.flight.requests.per.connection": 5,
            "transaction.timeout.ms": transaction_settings.timeout_ms,
        })

        self._consumer = Consumer({
            "bootstrap.servers": kafka_settings.bootstrap_servers,
            "group.id": "eos-processor-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
            "max.poll.interval.ms": 300000,
        })
        self._consumer.subscribe([self.INPUT_TOPIC])

        self._producer.init_transactions()
        logger.info("eos_processor_initialized", instance_id=instance_id)

    def run(self, max_messages: Optional[int] = None) -> int:
        """Main processing loop with exactly-once guarantees."""
        self._running = True
        processed = 0
        batch: list[Any] = []
        batch_size = 50

        while self._running:
            if max_messages and processed >= max_messages:
                break

            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                if batch:
                    processed += self._process_batch(batch)
                    batch = []
                continue

            if msg.error():
                continue

            batch.append(msg)

            if len(batch) >= batch_size:
                processed += self._process_batch(batch)
                batch = []

        if batch:
            processed += self._process_batch(batch)

        return processed

    def _process_batch(self, messages: list[Any]) -> int:
        """Process a batch within a single transaction."""
        try:
            self._producer.begin_transaction()
            count = 0

            for msg in messages:
                enriched = self._transform(msg)
                if enriched is not None:
                    self._producer.produce(
                        topic=self.OUTPUT_TOPIC,
                        key=msg.key(),
                        value=json.dumps(enriched).encode("utf-8"),
                        headers={"source_offset": str(msg.offset())},
                    )
                    count += 1

            positions = self._consumer.position(
                [TopicPartition(m.topic(), m.partition()) for m in messages]
            )
            self._producer.send_offsets_to_transaction(
                positions,
                self._consumer.consumer_group_metadata(),
            )
            self._producer.commit_transaction()

            EOS_PROCESSED.inc(count)
            EOS_TRANSACTIONS.labels(status="committed").inc()
            return count

        except KafkaException as e:
            logger.error("transaction_aborted", error=str(e))
            self._producer.abort_transaction()
            EOS_TRANSACTIONS.labels(status="aborted").inc()
            return 0

    def _transform(self, msg: Any) -> Optional[dict]:
        """Transform order placed into enriched order."""
        try:
            data = json.loads(msg.value().decode("utf-8"))
            data["enriched_at"] = time.time()
            data["processor_instance"] = self._instance_id
            data["is_high_value"] = float(data.get("total_amount", 0)) > 500.0
            return data
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    def stop(self) -> None:
        self._running = False

    def close(self) -> None:
        self._running = False
        self._consumer.close()
        self._producer.flush(timeout=10.0)
        logger.info("eos_processor_closed")


def main() -> None:
    """Run the exactly-once processor."""
    start_http_server(app_settings.metrics_port + 2)
    processor = ExactlyOnceProcessor(instance_id=0)

    def shutdown(signum: int, frame: Any) -> None:
        processor.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        processed = processor.run()
        logger.info("eos_complete", total_processed=processed)
    finally:
        processor.close()


if __name__ == "__main__":
    main()
