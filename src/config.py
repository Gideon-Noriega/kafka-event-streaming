"""Centralized configuration management using pydantic-settings."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    """Kafka connection and behavior configuration."""

    model_config = SettingsConfigDict(env_prefix="KAFKA_", env_file=".env")

    bootstrap_servers: str = "localhost:19092"
    schema_registry_url: str = "http://localhost:18081"


class ProducerSettings(BaseSettings):
    """Producer-specific configuration."""

    model_config = SettingsConfigDict(env_prefix="PRODUCER_", env_file=".env")

    client_id: str = "order-producer"
    compression: str = "lz4"
    linger_ms: int = 20
    batch_size: int = 65536
    acks: str = "all"
    max_in_flight: int = 5
    retries: int = 2147483647
    retry_backoff_ms: int = 100


class ConsumerSettings(BaseSettings):
    """Consumer-specific configuration."""

    model_config = SettingsConfigDict(env_prefix="CONSUMER_", env_file=".env")

    group_id: str = "order-processing-group"
    auto_offset_reset: str = "earliest"
    max_poll_records: int = 500
    max_poll_interval_ms: int = 300000
    session_timeout_ms: int = 45000
    heartbeat_interval_ms: int = 3000


class TransactionSettings(BaseSettings):
    """Transaction configuration."""

    model_config = SettingsConfigDict(env_prefix="TRANSACTION_", env_file=".env")

    timeout_ms: int = 60000
    transactional_id_prefix: str = "order-processor"


class DLQSettings(BaseSettings):
    """Dead Letter Queue configuration."""

    model_config = SettingsConfigDict(env_prefix="DLQ_", env_file=".env")

    topic: str = "orders.dlq"
    max_retries: int = 3
    retry_backoff_ms: int = 1000


class AppSettings(BaseSettings):
    """Application-level settings."""

    model_config = SettingsConfigDict(env_file=".env")

    log_level: str = "INFO"
    metrics_port: int = 9090
    api_port: int = 8000


# Singleton instances
kafka_settings = KafkaSettings()
producer_settings = ProducerSettings()
consumer_settings = ConsumerSettings()
transaction_settings = TransactionSettings()
dlq_settings = DLQSettings()
app_settings = AppSettings()
