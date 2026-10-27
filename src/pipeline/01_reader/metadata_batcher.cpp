#include "pipeline/01_reader/metadata_batcher.hpp"
#include "pipeline/shared/pipeline_context.hpp"
#include "pipeline/shared/stage_support.hpp"

namespace pbe::detail {
using Clock = PipelineClock;

void MetadataBatcher::flush_metadata() {
    if (pending_metadata.empty())
        return;

    // Move the batch out and reset the accumulator so the Reader can build the next batch.
    auto batch = std::move(pending_metadata);
    pending_metadata = {};
    const auto begin = Clock::now();

    // Reader -> Stage 04 handoff; a full queue applies Metadata backpressure to the Reader.
    if (!context.metadata_queue.push(std::move(batch), &context.errors.cancelled)) {
        context.errors.rethrow();
        throw std::runtime_error("metadata queue closed during backup");
    }
    context.metrics.metadata_queue_wait_ns.fetch_add(elapsed_ns(begin), std::memory_order_relaxed);
}

void MetadataBatcher::queue_entry(const SourceEntry& entry) {
    pending_metadata.entries.push_back(DbEntry{entry.relative, entry.kind, entry.symlink_target,
                                               entry.size, entry.mode, entry.mtime_ns});
    if (pending_metadata.entries.size() >= 128)
        flush_metadata();
}

void MetadataBatcher::queue_chunk(const std::string& path, std::uint64_t ordinal,
                                  std::uint64_t bytes, std::shared_future<ObjectHeader> result) {
    pending_metadata.chunks.push_back(PendingChunk{path, ordinal, bytes, std::move(result)});
    if (pending_metadata.chunks.size() >= context.coordinator_batch_size)
        flush_metadata();
}
} // namespace pbe::detail
