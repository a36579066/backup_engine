# Milestone 4: Streaming object packs

Pack-v2 streams objects toward a 128 MiB target or 256-object cap, releasing each payload and memory credit after staging. It publishes after reaching its limit or 2 ms idle. The coordinator still waits at each file boundary.

The reader/coordinator, CPU workers, and storage stage use bounded queues, RAII credits, explicit cancellation, and close/join cleanup. SQLite references are committed only after durable ACKs.
