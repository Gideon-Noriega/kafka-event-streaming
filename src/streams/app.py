"""Faust streaming application entry point."""
from __future__ import annotations
import faust
from src.config import kafka_settings

app = faust.App(
    "order-processing",
    broker=f"kafka://{kafka_settings.bootstrap_servers}",
    store="memory://",
    topic_replication_factor=1,
    stream_buffer_maxsize=10000,
    producer_compression_type="lz4",
)

from src.streams.enrichment import OrderEnrichmentStream  # noqa: E402, F401
from src.streams.fraud_detection import FraudDetectionStream  # noqa: E402, F401
from src.streams.state_machine import OrderStateMachine  # noqa: E402, F401


def main() -> None:
    app.main()


if __name__ == "__main__":
    main()
