# Kafka Event Streaming Platform

Production-grade event streaming demonstrating advanced Apache Kafka patterns for e-commerce order processing.

## Architecture



## Features

### Producer Patterns
- **Idempotent Producer** - exactly-once with enable.idempotence=true
- **Custom Partitioning** - region-aware routing (murmur2 hash)
- **Batching and Compression** - linger.ms=20, batch.size=64KB, LZ4
- **Transactional Producer** - atomic read-process-write

### Consumer Patterns
- **Consumer Groups** - cooperative-sticky assignor, graceful rebalancing
- **Exactly-Once Semantics** - transactional consume-transform-produce
- **Manual Offset Management** - periodic commit with checkpoints
- **Dead Letter Queue** - exponential backoff, error classification

### Stream Processing (Faust)
- **KStream-KTable Join** - order enrichment from customer profiles
- **Windowed Aggregation** - 5min tumbling window fraud detection
- **State Machine** - validated transitions, event sourcing

### Schema Management
- **Avro Schemas** with Schema Registry
- **Schema Evolution** - backward-compatible v1 to v2
- **TopicNameStrategy** subject naming

### Operations
- Topic configs (retention, compaction, partitions)
- Kafka Connect (Debezium CDC, ES sink, S3 Parquet)
- Prometheus metrics (lag, throughput, errors)

## Quick Start



## Development



## Topics

| Topic | Partitions | Retention | Key |
|-------|-----------|-----------|-----|
| orders.placed | 12 | 7d | customer_id |
| orders.enriched | 12 | 7d | order_id |
| orders.fraud_alerts | 3 | 30d | customer_id |
| orders.dlq | 1 | 90d | original_key |
| payments.processed | 6 | 7d | order_id |
| shipments.created | 6 | 7d | order_id |
| customers.profiles | 6 | compact | customer_id |

## Structure



## License

MIT
