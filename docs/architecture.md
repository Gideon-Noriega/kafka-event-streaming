# Architecture

## Event-Driven Order Processing

This system implements an event-driven architecture for e-commerce order
processing using Apache Kafka as the central event backbone.

## Design Principles

### 1. Event Sourcing
All state changes are captured as immutable events. The current state of any
order can be reconstructed by replaying its event stream.

### 2. CQRS
Write path (producers) and read path (consumers) are separated:
- Writes go through validated producers to Kafka topics
- Reads come from materialized views (Elasticsearch, PostgreSQL)

### 3. Exactly-Once Semantics
- Idempotent producers (PID + sequence numbers)
- Transactional consume-transform-produce
- Consumer offset commits within transactions

### 4. Schema Evolution
Avro schemas with Schema Registry ensure:
- Type safety across producer/consumer versions
- Backward-compatible evolution (new fields have defaults)
- Independent service deployment

## Failure Handling

### Dead Letter Queue Pattern
Messages that fail processing after N retries are routed to orders.dlq:
1. Classify error (transient vs permanent)
2. Retry transient errors with exponential backoff
3. Route to DLQ with full failure metadata
4. Alert operations team for manual remediation

## Partitioning Strategy

### Region-Aware Partitioning
- Partitions 0-2: US East
- Partitions 3-5: US West
- Partitions 6-7: EU West
- Partitions 8-9: EU Central
- Partitions 10-11: APAC

## Monitoring

### Key Metrics
- Producer: messages/sec, p99 latency, error rate
- Consumer: lag per partition, processing time, rebalance frequency
- Streams: records processed, state store size

### Alerting Thresholds
- Consumer lag > 10000: WARNING
- Consumer lag > 100000: CRITICAL
- Producer error rate > 1%: WARNING
- Rebalance frequency > 1/min: WARNING
