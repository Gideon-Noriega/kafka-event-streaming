"""High-throughput batched producer with compression tuning.

Demonstrates maximizing producer throughput through:
- Aggressive batching (linger.ms, batch.size)
- LZ4/Snappy/ZSTD compression comparison
- Async production with bounded buffers
- Throughput metrics and backpressure handling
"""

from __future__ import annotations

import time
from typing import Any, Callable, Optional

import structlog
from confluent_kafka import KafkaError, Producer
from prometheus_client import Counter

from src.config import kafka_settings

logger = structlog.get_logger(__name__)

THROUGHPUT_COUNTER = Counter("kafka_producer_messages_sent_total", "Total messages sent")


class BatchedProducer:
    """High-throughput producer: linger.ms=50, batch.size=128KB, LZ4 compression.

    Use for high-volume ingestion where sub-100ms latency is acceptable.
    """

    def __init__(self, topic: str, compression: str = "lz4",
                 linger_ms: int = 50, batch_size: int = 131072) -> None:
        self._topic = topic
        self._total_produced = 0
        self._last_flush = time.time()
        self._producer = Producer({
            "bootstrap.servers": kafka_settings.bootstrap_servers,
            "client.id": f"batched-producer-{topic}",
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": linger_ms,
            "batch.size": batch_size,
            "batch.num.messages": 10000,
            "compression.type": compression,
            "queue.buffering.max.messages": 500000,
            "queue.buffering.max.kbytes": 131072,
            "socket.send.buffer.bytes": 1048576,
        })
        logger.info("batched_producer_initialized", topic=topic, compression=compression)

    def produce_many(self, messages: list[tuple[str, bytes]],
                     flush_interval: float = 1.0,
                     on_progress: Optional[Callable[[int, int], None]] = None) -> int:
        """Produce many messages with periodic flushing and backpressure."""
        total = len(messages)
        queued = 0

        for i, (key, value) in enumerate(messages):
            try:
                self._producer.produce(
                    topic=self._topic,
                    key=key.encode("utf-8") if isinstance(key, str) else key,
                    value=value,
                )
                queued += 1
            except BufferError:
                logger.warning("backpressure_flush")
                self._producer.flush(timeout=10.0)
                self._producer.produce(
                    topic=self._topic,
                    key=key.encode("utf-8") if isinstance(key, str) else key,
                    value=value,
                )
                queued += 1

            if time.time() - self._last_flush >= flush_interval:
                self._producer.flush(timeout=5.0)
                self._last_flush = time.time()
            self._producer.poll(0)

            if on_progress and (i + 1) % 1000 == 0:
                on_progress(i + 1, total)

        self._producer.flush(timeout=30.0)
        self._total_produced += queued
        THROUGHPUT_COUNTER.inc(queued)
        logger.info("batch_complete", queued=queued, total=total)
        return queued

    def close(self) -> None:
        self._producer.flush(timeout=30.0)
        logger.info("batched_producer_closed", total_produced=self._total_produced)


def benchmark_compression() -> None:
    """Benchmark different compression codecs for throughput comparison."""
    import json
    import random
    import string

    def make_payload() -> bytes:
        return json.dumps({
            "event_type": "page_view",
            "user_id": f"user-{random.randint(1, 10000)}",
            "page": "/products/" + "".join(random.choices(string.ascii_lowercase, k=8)),
            "timestamp": time.time(),
        }).encode("utf-8")

    messages = [(f"key-{i}", make_payload()) for i in range(10000)]

    for codec in ["none", "lz4", "snappy", "zstd"]:
        producer = BatchedProducer(topic="benchmark.test", compression=codec)
        start = time.time()
        queued = producer.produce_many(messages)
        elapsed = time.time() - start
        throughput = queued / elapsed if elapsed > 0 else 0
        logger.info("benchmark_result", codec=codec, throughput_msg_s=round(throughput))
        producer.close()


if __name__ == "__main__":
    benchmark_compression()
