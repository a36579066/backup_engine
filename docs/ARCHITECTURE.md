# Milestone 1: CPU workers with one object writer

Raw chunks pass through a bounded CPU queue for BLAKE3, deduplication and Zstd. One storage thread durably publishes loose objects. The coordinator commits SQLite references after acknowledgements. Only files-v1 and one writer are supported.

The reader/coordinator, CPU workers, and storage stage use bounded queues, RAII credits, explicit cancellation, and close/join cleanup. SQLite references are committed only after durable ACKs.
