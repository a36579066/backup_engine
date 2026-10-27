#include "pipeline/01_reader/source_io_scheduler.hpp"
#include "pipeline/01_reader/metadata_batcher.hpp"
#include "pipeline/shared/pipeline_context.hpp"
#include "pipeline/shared/stage_support.hpp"

#include <algorithm>
#include <stdexcept>

namespace pbe::detail {
SourceIoScheduler::SourceIoScheduler(PipelineContext& context, MetadataBatcher& metadata)
    : context_(context),
      metadata_(metadata) {
    pending_.reserve(std::min<std::size_t>(context.session.plan.io_policy.batch_chunks, 128));
}

void SourceIoScheduler::check_cancelled() const {
    if (context_.errors.cancelled.load(std::memory_order_relaxed)) {
        context_.errors.rethrow();
        throw std::runtime_error("backup cancelled during I/O scheduling");
    }
}

MemoryBudget::Credit SourceIoScheduler::acquire_credit() {
    const auto& policy = context_.session.plan.io_policy;
    if (policy.phased && pending_.size() == policy.batch_chunks)
        flush();
    check_cancelled();
    return context_.memory.acquire(context_.credit_per_chunk, context_.errors.cancelled);
}

void SourceIoScheduler::submit(const std::string& path, std::uint64_t ordinal,
                               std::vector<std::uint8_t> raw, MemoryBudget::Credit credit) {
    check_cancelled();
    auto promise = std::make_shared<std::promise<ObjectHeader>>();
    auto result = promise->get_future().share();
    const auto bytes = raw.size();
    PendingRead read{{ordinal, std::move(raw), std::move(credit), std::move(promise)},
                     {path, ordinal, bytes, std::move(result)}};
    if (context_.session.plan.io_policy.phased)
        pending_.push_back(std::move(read));
    else
        dispatch(read);
}

void SourceIoScheduler::dispatch(PendingRead& read) {
    // Stage 01 -> 02 handoff; Metadata and the I/O scheduler can share the same completion result.
    if (!context_.input_queue.push(std::move(read.task), &context_.errors.cancelled)) {
        context_.errors.rethrow();
        throw std::runtime_error("chunk queue closed during I/O scheduling");
    }
    const auto& chunk = read.metadata;
    metadata_.queue_chunk(chunk.path, chunk.ordinal, chunk.bytes, chunk.result);
}

void SourceIoScheduler::flush() {
    if (pending_.empty())
        return;
    check_cancelled();
    const auto begin = PipelineClock::now();
    context_.metrics.io_draining.store(true, std::memory_order_relaxed);
    try {
        for (auto& read : pending_)
            dispatch(read);
        metadata_.flush_metadata();

        // Wait for every durable ACK, not merely an empty queue or staged payload bytes.
        // Only source payload reads and object writes are phased; SQLite and OS writeback may overlap.
        for (const auto& read : pending_) {
            while (read.metadata.result.wait_for(std::chrono::milliseconds(10)) !=
                   std::future_status::ready)
                check_cancelled();
            check_cancelled();
            (void)read.metadata.result.get(); // Propagate failed ACKs as well.
        }
    } catch (...) {
        context_.metrics.io_draining.store(false, std::memory_order_relaxed);
        throw;
    }
    context_.metrics.io_draining.store(false, std::memory_order_relaxed);
    context_.metrics.io_drain_wait_ns.fetch_add(elapsed_ns(begin), std::memory_order_relaxed);
    context_.metrics.io_batches.fetch_add(1, std::memory_order_relaxed);
    pending_.clear();
}
} // namespace pbe::detail
