# Milestone 2: Concurrent object-writer pool

A configurable pool of 1 to 32 storage workers durably publishes loose objects. CPU workers release admission before waiting for object ACKs. Only files-v1 is supported.

The reader/coordinator, CPU workers, and storage stage use bounded queues, RAII credits, explicit cancellation, and close/join cleanup. SQLite references are committed only after durable ACKs.
