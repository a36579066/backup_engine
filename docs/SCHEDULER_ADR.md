# ADR-11: Fixed baseline and opt-in adaptive CPU admission

**Status:** Accepted.

PBE defaults to a fixed number of CPU workers. Adaptive mode creates the same fixed-size pool and changes only the number of workers admitted into hashing and compression. It starts at one permit, samples every two seconds, applies one-permit steps, uses warmup and cooldown windows, and records every observation and action.

Writer and memory pressure may decrease the active limit. CPU pressure can increase it only when the input queue has work and output queues have capacity. Unknown or mixed observations hold the current limit. A sustained regression rollback is best effort and only attempted when adjacent windows have enough work and matching diagnoses.

This design preserves a fixed reproducible baseline and keeps scheduling decisions outside object and metadata correctness paths.
