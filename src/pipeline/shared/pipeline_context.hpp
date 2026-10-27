#pragma once

#include "core/backup_session.hpp"
#include "concurrency/claim_registry.hpp"
#include "concurrency/memory_budget.hpp"
#include "concurrency/worker_error.hpp"
#include "diagnostics/pipeline_metrics.hpp"
#include "pipeline/shared/messages.hpp"
#include "scheduler/active_worker_gate.hpp"
#include "pbe/bounded_queue.hpp"

#include <fstream>
#include <future>
#include <memory>
#include <vector>

namespace pbe::detail {
// Shared run resources must outlive every worker and asynchronous completion callback.
// Stages own local tasks; queues, credits, claims, and atomic counters coordinate shared work.
// Only the Metadata Writer uses the session's catalog for writes while workers are running.
struct PipelineContext {
    explicit PipelineContext(BackupSession& session);
    void fail(std::exception_ptr error);

    // Borrow session-owned resources instead of duplicating them; the session must outlive this context.
    BackupSession& session;
    const BackupOptions& options;
    const std::filesystem::path& repo;
    const std::filesystem::path& source;
    const std::string& snapshot_id;
    const std::string& run_id;
    const std::string& source_text;
    const std::string& job_id;
    Catalog& catalog;
    BackupSummary& summary;

    // Immutable configuration, derived before starting workers.
    const bool packed_storage;
    const std::size_t workers_count;
    const std::size_t credit_per_chunk;
    const std::size_t coordinator_batch_size;

    // Memory bounds in-flight buffers; the gate limits simultaneous CPU work.
    MemoryBudget memory;
    ActiveWorkerGate gate;

    // Data path: 01 -> input_queue -> 02 -> object_queue -> 03.
    // Metadata path: 01 -> metadata_queue -> 04, which waits for 02/03 results before writing SQLite.
    BoundedQueue<ChunkTask> input_queue;
    BoundedQueue<BlobWriteTask> object_queue;
    BoundedQueue<MetadataBatch> metadata_queue;

    // Claims coordinate duplicates; the bounded verification cache avoids repeated checks of recent objects.
    ClaimRegistry<ObjectHeader> claims;
    VerifiedObjectCache verified_existing_objects;

    // Workers may update shared error state and diagnostic counters concurrently.
    WorkerError errors;
    PipelineMetrics metrics;

    // Give each CPU worker a private read connection instead of sharing one SQLite connection.
    std::vector<std::unique_ptr<Catalog>> object_index_readers;
    std::ofstream trace;
};
} // namespace pbe::detail
