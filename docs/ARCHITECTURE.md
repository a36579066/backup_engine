# Milestone 3: Memory-bounded batch packs

Pack-v2 collects a bounded in-memory batch, writes its objects to one immutable pack, and acknowledges the batch after durable publication. Batch size is derived from the memory credit capacity. Files-v1 remains supported.

The reader/coordinator, CPU workers, and storage stage use bounded queues, RAII credits, explicit cancellation, and close/join cleanup. SQLite references are committed only after durable ACKs.
