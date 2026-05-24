"""Manual offset management strategies."""
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Optional
import structlog
from confluent_kafka import Consumer, KafkaError, TopicPartition
from src.config import kafka_settings

logger = structlog.get_logger(__name__)


class OffsetManager:
    """Manual offset management with external checkpoint support.

    Strategies:
    1. Sync commit after each message (safest, slowest)
    2. Sync commit after N messages (balanced)
    3. Async commit with periodic sync (fastest, risk of reprocessing)
    4. External checkpoint (for exactly-once with non-Kafka sinks)
    """

    def __init__(self, topic: str, group_id: str, commit_interval: int = 100,
                 checkpoint_file: Optional[str] = None) -> None:
        self._topic = topic
        self._commit_interval = commit_interval
        self._messages_since_commit = 0
        self._checkpoint_file = Path(checkpoint_file) if checkpoint_file else None
        self._partition_offsets: dict[int, int] = {}
        self._consumer = Consumer({
            "bootstrap.servers": kafka_settings.bootstrap_servers,
            "group.id": group_id, "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self._consumer.subscribe([topic])
        if self._checkpoint_file and self._checkpoint_file.exists():
            self._load_checkpoint()

    def consume_with_periodic_commit(self, max_messages: int = 1000) -> int:
        """Consume with periodic synchronous commits every N messages."""
        processed = 0
        while processed < max_messages:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                continue
            self._handle_message(msg)
            processed += 1
            self._messages_since_commit += 1
            self._partition_offsets[msg.partition()] = msg.offset() + 1
            if self._messages_since_commit >= self._commit_interval:
                self._sync_commit()
                self._messages_since_commit = 0
        if self._messages_since_commit > 0:
            self._sync_commit()
        return processed

    def seek_to_offset(self, partition: int, offset: int) -> None:
        tp = TopicPartition(self._topic, partition, offset)
        self._consumer.seek(tp)

    def seek_to_beginning(self, partitions=None):
        assignment = self._consumer.assignment()
        if partitions:
            assignment = [tp for tp in assignment if tp.partition in partitions]
        for tp in assignment:
            tp.offset = 0
            self._consumer.seek(tp)

    def _sync_commit(self):
        try:
            self._consumer.commit(asynchronous=False)
            if self._checkpoint_file:
                self._save_checkpoint()
        except Exception as e:
            logger.error("commit_failed", error=str(e))

    def _save_checkpoint(self):
        if self._checkpoint_file:
            self._checkpoint_file.write_text(json.dumps(self._partition_offsets))

    def _load_checkpoint(self):
        if self._checkpoint_file and self._checkpoint_file.exists():
            offsets = json.loads(self._checkpoint_file.read_text())
            self._partition_offsets = {int(k): v for k, v in offsets.items()}

    def _handle_message(self, msg):
        pass

    def close(self):
        self._sync_commit()
        self._consumer.close()
