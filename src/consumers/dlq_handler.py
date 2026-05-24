"""Dead Letter Queue handler with retry policies."""
from __future__ import annotations
import json, time
from datetime import datetime
from typing import Any, Callable, Optional
import structlog
from confluent_kafka import Consumer, KafkaError, Producer
from prometheus_client import Counter
from src.config import kafka_settings, consumer_settings, dlq_settings

logger = structlog.get_logger(__name__)
DLQ_MESSAGES = Counter("kafka_dlq_messages_total", "DLQ messages", ["reason"])
RETRY_ATTEMPTS = Counter("kafka_retry_attempts_total", "Retries", ["topic"])


class DeadLetterQueueHandler:
    """Routes failed messages to DLQ with retry logic."""
    TRANSIENT_ERRORS = {"timeout", "connection_reset", "broker_unavailable"}
    PERMANENT_ERRORS = {"deserialization_error", "schema_mismatch", "validation_error"}

    def __init__(self, source_topic: str, process_fn: Callable[[Any], None],
                 group_id: str = "dlq-handler-group") -> None:
        self._source_topic = source_topic
        self._process_fn = process_fn
        self._running = False
        self._consumer = Consumer({
            "bootstrap.servers": kafka_settings.bootstrap_servers,
            "group.id": group_id, "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": consumer_settings.max_poll_interval_ms,
        })
        self._consumer.subscribe([source_topic])
        self._dlq_producer = Producer({
            "bootstrap.servers": kafka_settings.bootstrap_servers,
            "client.id": "dlq-producer", "enable.idempotence": True, "acks": "all",
        })

    def run(self, max_messages=None):
        self._running = True
        processed = dlq_count = 0
        while self._running:
            if max_messages and (processed + dlq_count) >= max_messages:
                break
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                continue
            if self._process_with_retry(msg):
                self._consumer.commit(message=msg, asynchronous=False)
                processed += 1
            else:
                self._send_to_dlq(msg)
                self._consumer.commit(message=msg, asynchronous=False)
                dlq_count += 1
        return processed, dlq_count

    def _process_with_retry(self, msg):
        attempts = 0
        while attempts < dlq_settings.max_retries:
            try:
                self._process_fn(msg)
                return True
            except Exception as e:
                attempts += 1
                error_type = self._classify_error(e)
                RETRY_ATTEMPTS.labels(topic=self._source_topic).inc()
                if error_type in self.PERMANENT_ERRORS:
                    return False
                if attempts < dlq_settings.max_retries:
                    backoff = dlq_settings.retry_backoff_ms * (2 ** (attempts - 1)) / 1000
                    time.sleep(backoff)
        return False

    def _classify_error(self, error):
        name = type(error).__name__.lower()
        if any(t in name for t in ["timeout", "connection", "unavailable"]):
            return "timeout"
        if any(t in name for t in ["decode", "serialize", "schema", "validation"]):
            return "deserialization_error"
        return "unknown"

    def _send_to_dlq(self, msg):
        record = {"original_topic": msg.topic(), "original_partition": msg.partition(),
            "original_offset": msg.offset(),
            "original_key": msg.key().decode("utf-8") if msg.key() else None,
            "error_timestamp": datetime.utcnow().isoformat(),
            "retry_count": dlq_settings.max_retries}
        self._dlq_producer.produce(topic=dlq_settings.topic, key=msg.key(),
            value=json.dumps(record).encode("utf-8"),
            headers={"original_topic": msg.topic(), "failure_reason": "max_retries_exceeded"})
        self._dlq_producer.flush(timeout=5.0)
        DLQ_MESSAGES.labels(reason="max_retries_exceeded").inc()

    def stop(self):
        self._running = False

    def close(self):
        self._running = False
        self._consumer.close()
        self._dlq_producer.flush(timeout=10.0)
