#include "pipeline/shared/pipeline_context.hpp"

#include <algorithm>
#include <iostream>

namespace pbe::detail {
namespace {
constexpr std::size_t kBytesPerMiB = std::size_t{1} << 20;
constexpr std::size_t kMinimumInputQueueCapacity = 2;
constexpr std::size_t kInputQueueSlotsPerCpuWorker = 2;
constexpr std::size_t kMinimumMetadataChunkBatch = 8;
constexpr std::size_t kMetadataChunksPerCpuWorker = 4;
constexpr std::size_t kMetadataQueueCapacity = 4;
constexpr std::size_t kVerifiedObjectCacheCapacity = 4096;

std::size_t metadata_chunk_batch_size(const BackupPlan& plan) {
    if (plan.packed_storage)
        return kPackMaxObjects * plan.options.writer_workers;

    return std::max(kMinimumMetadataChunkBatch, plan.workers_count * kMetadataChunksPerCpuWorker);
}

std::size_t cpu_input_queue_capacity(std::size_t workers) {
    return std::max(kMinimumInputQueueCapacity, workers * kInputQueueSlotsPerCpuWorker);
}

std::size_t storage_queue_capacity(bool packed_storage, std::size_t input_capacity,
                                   std::size_t metadata_batch_size) {
    // Packed storage can gather a full metadata batch; loose storage follows the input capacity.
    if (packed_storage)
        return std::max(input_capacity, metadata_batch_size);

    return input_capacity;
}
} // namespace

// Credits cover raw and worst-case compressed buffers; queue bounds limit pending task counts.
PipelineContext::PipelineContext(BackupSession& owner)
    : session(owner),
      options(owner.plan.options),
      repo(owner.plan.repo),
      source(owner.plan.source),
      snapshot_id(owner.snapshot_id),
      run_id(owner.run_id),
      source_text(owner.source_text),
      job_id(owner.job_id),
      catalog(owner.catalog),
      summary(owner.summary),

      // Immutable run configuration.
      packed_storage(owner.plan.packed_storage),
      workers_count(owner.plan.workers_count),
      credit_per_chunk(owner.plan.io_policy.chunk_credit_bytes),
      coordinator_batch_size(metadata_chunk_batch_size(owner.plan)),

      // Admission controls for buffer allocation and CPU work.
      memory(options.memory_mib * kBytesPerMiB),
      gate(workers_count, options.adaptive ? 1 : workers_count),

      // Bounded handoff queues and the recent-verification cache.
      input_queue(cpu_input_queue_capacity(workers_count)),
      object_queue(
          storage_queue_capacity(packed_storage, input_queue.capacity(), coordinator_batch_size)),
      metadata_queue(kMetadataQueueCapacity),
      verified_existing_objects(kVerifiedObjectCacheCapacity),
      trace(repo / "reports" / (run_id + ".jsonl"), std::ios::out | std::ios::app) {
    metrics.process_rss_peak_bytes.store(current_rss_bytes(), std::memory_order_relaxed);
    if (!trace)
        std::cerr
            << "warning: decision trace could not be opened; backup correctness is unaffected\n";
}

void PipelineContext::fail(std::exception_ptr error) {
    // Setting cancelled alone cannot wake every waiter; also close the queues and CPU gate.
    errors.set(error);
    input_queue.close();
    object_queue.close();
    metadata_queue.close();
    gate.close();
}
} // namespace pbe::detail
