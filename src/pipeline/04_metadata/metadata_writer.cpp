#include "pipeline/04_metadata/metadata_writer.hpp"
#include "pipeline/04_metadata/metadata_commit.hpp"
#include "pipeline/shared/pipeline_context.hpp"

namespace pbe::detail {
void run_metadata_writer(PipelineContext& context) {
    auto& options = context.options;
    auto& snapshot_id = context.snapshot_id;
    auto& catalog = context.catalog;
    auto& summary = context.summary;
    auto& metadata_queue = context.metadata_queue;
    auto& claims = context.claims;
    auto& errors = context.errors;
    auto& metrics = context.metrics;
    const auto fail = [&context](std::exception_ptr error) {
        context.fail(error);
    };
    try {
        while (auto batch = metadata_queue.pop(&errors.cancelled)) {
            if (errors.cancelled.load(std::memory_order_relaxed))
                break;

            // Wait for this batch's durable results; other workers can process later queued chunks.
            // Streaming Readers may keep reading while queues/credits allow; phased Readers drain.
            const auto committed = detail::commit_metadata_batch(
                catalog.get(), snapshot_id, *batch, options.failpoint, errors.cancelled);
            summary.unique_raw_bytes += committed.unique_raw_bytes;
            summary.reused_chunks += committed.reused_chunks;
            metrics.catalog_commit_ns.fetch_add(committed.transaction_ns,
                                                std::memory_order_relaxed);
            metrics.metadata_ack_wait_ns.fetch_add(committed.ack_wait_ns,
                                                   std::memory_order_relaxed);
            if (!batch->entries.empty() || !batch->chunks.empty())
                metrics.metadata_batches.fetch_add(1, std::memory_order_relaxed);

            // Remove claims only after commit so later workers can find their object locations in SQLite.
            for (const auto& id : committed.object_ids)
                claims.release(id);
            metrics.progress_file_count.fetch_add(batch->completed_files,
                                                  std::memory_order_relaxed);
        }
    } catch (...) {
        fail(std::current_exception());
    }
}

} // namespace pbe::detail
