# Roadmap

## Streaming with Debezium and Kafka

Explore a separate streaming version to learn skills beyond the current
five-minute microbatch pipeline:

- Replace AWS DMS with Debezium to capture PostgreSQL WAL changes continuously.
- Send change events through Kafka to learn topics, partitions, consumer offsets
  and replay.
- Add a streaming consumer to practice continuous change processing, ordered
  updates, delete handling and recovery after failures.

This is a future learning goal, not an implemented feature. The current DMS
pipeline already demonstrates real log-based CDC. Debezium and Kafka can also
feed microbatches; adding them alone would not make our downstream processing
continuous.
