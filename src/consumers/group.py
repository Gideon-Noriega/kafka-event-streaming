"""Consumer group with cooperative sticky rebalancing.

Demonstrates:
- Consumer group membership and partition assignment
- Cooperative sticky assignor for minimal rebalancing disruption
- Graceful shutdown with partition revocation handling
- Per-partition offset tracking and manual commit
"""

from __future__ import annotations

import signal
import sys
import time
from typing import Any, Optional

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from src.config import kafka_settings, consumer_settings, app_settings

logger = structlog.get_logger(__name__)

MESSAGES_CONSUMED = Counter("kafka_messages_consumed_total", "Messages consumed", ["topic", "partition"])
CONSUMER_LAG = Gauge("kafka_consumer_lag", "Consumer lag", ["topic", "partition"])
PROCESSING_TIME = Histogram("kafka_message_processing_seconds", "Processing time per message")


class OrderConsumerGroup:
    """Consumer group with cooperative rebalancing for order fulfillment.

    Key design decisions:
    - partition.assignment.strategy=cooperative-sticky: Incremental rebalancing
      only revokes partitions that must move, keeping other partitions processing
    - enable.auto.commit=false: Manual offset commit after successful processing
    - max.poll.interval.ms=300000: 5 min timeout for long-running processing
    - session.timeout.ms=45000: Balance between failure detection and stability
    """

    TOPICS = ["orders.enriched", "payments.processed"]

    def __init__(self, group_id: Optional[str] = None) -> None:
        self._running = False
        self._assigned_partitions: list[TopicPartition] = []

        conf = {
            "bootstrap.servers": kafka_settings.bootstrap_servers,
            "group.id": group_id or consumer_settings.group_id,
            "auto.offset.reset": consumer_settings.auto_offset_reset,
            "enable.auto.commit": False,
            "max.poll.interval.ms": consumer_settings.max_poll_interval_ms,
            "session.timeout.ms": consumer_settings.session_timeout_ms,
            "heartbeat.interval.ms": consumer_settings.heartbeat_interval_ms,
            "partition.assignment.strategy": "cooperative-sticky",
            "fetch.min.bytes": 1024,
            "fetch.max.wait.ms": 500,
        }
        self._consumer = Consumer(conf)
        self._consumer.subscribe(
            self.TOPICS,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )
        logger.info("consumer_group_initialized", group_id=conf["group.id"])

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Called when partitions are assigned to this consumer."""
        logger.info("partitions_assigned",
                    partitions=[(p.topic, p.partition) for p in partitions])
        self._assigned_partitions.extend(partitions)

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Called when partitions are revoked (cooperative rebalancing)."""
        logger.info("partitions_revoked",
                    partitions=[(p.topic, p.partition) for p in partitions])
        try:
            consumer.commit(offsets=partitions, asynchronous=False)
        except KafkaException:
            pass
        for p in partitions:
            if p in self._assigned_partitions:
                self._assigned_partitions.remove(p)

    def _on_lost(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Called when partitions are lost (broker failure)."""
        logger.warning("partitions_lost",
                       partitions=[(p.topic, p.partition) for p in partitions])
        self._assigned_partitions = [
            p for p in self._assigned_partitions if p not in partitions
        ]

    def run(self, max_messages: Optional[int] = None) -> int:
        """Main consume loop with manual offset management."""
        self._running = True
        processed = 0

        while self._running:
            if max_messages and processed >= max_messages:
                break

            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("consumer_error", error=msg.error().str())
                continue

            start = time.time()
            try:
                self._process_message(msg)
                self._consumer.commit(message=msg, asynchronous=False)
                processed += 1
                MESSAGES_CONSUMED.labels(
                    topic=msg.topic(), partition=str(msg.partition())
                ).inc()
                PROCESSING_TIME.observe(time.time() - start)
            except Exception as e:
                logger.error("processing_failed", error=str(e),
                             topic=msg.topic(), partition=msg.partition(),
                             offset=msg.offset())

        return processed

    def _process_message(self, msg: Any) -> None:
        """Process a single message (override in subclasses)."""
        import json
        data = json.loads(msg.value().decode("utf-8"))
        logger.debug("message_processed", topic=msg.topic(),
                     partition=msg.partition(), offset=msg.offset())

    def stop(self) -> None:
        self._running = False

    def close(self) -> None:
        self._running = False
        self._consumer.close()
        logger.info("consumer_closed")


def main() -> None:
    """Run consumer group with graceful shutdown."""
    start_http_server(app_settings.metrics_port + 1)
    consumer = OrderConsumerGroup()

    def shutdown(signum: int, frame: Any) -> None:
        consumer.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        processed = consumer.run()
        logger.info("consumer_finished", total_processed=processed)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
