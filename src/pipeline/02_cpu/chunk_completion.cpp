#include "pipeline/02_cpu/chunk_completion.hpp"
#include "pipeline/shared/pipeline_context.hpp"
#include "pipeline/shared/stage_support.hpp"

namespace pbe::detail {
using Clock = PipelineClock;

void subscribe_to_duplicate(PipelineContext& context, const std::shared_ptr<ChunkClaim>& state,
                            const std::shared_ptr<std::promise<ObjectHeader>>& promise,
                            std::size_t raw_size) {
    const auto wait_begin = Clock::now();

    // Completed claims may invoke immediately; otherwise the completing thread invokes the callback.
    context.claims.when_ready(state, [&context, promise, raw_size, wait_begin](
                                         const ObjectHeader* durable, std::exception_ptr error) {
        context.metrics.duplicate_ack_wait_ns.fetch_add(elapsed_ns(wait_begin),
                                                        std::memory_order_relaxed);
        try {
            if (error) {
                promise->set_exception(error);
                return;
            }
            if (!durable)
                throw std::runtime_error("duplicate claim returned no object");
            auto header = *durable;
            header.newly_written = false; // Duplicate references are not new content.
            context.metrics.completed_chunks.fetch_add(1, std::memory_order_relaxed);
            context.metrics.completed_raw_bytes.fetch_add(raw_size, std::memory_order_relaxed);
            promise->set_value(std::move(header));
        } catch (...) {
            try {
                promise->set_exception(std::current_exception());
            } catch (...) {
            }
        }
    });
    context.metrics.claim_wait_ns.fetch_add(elapsed_ns(wait_begin), std::memory_order_relaxed);
}

void submit_new_object(PipelineContext& context, ChunkTask& task, BlobWriteTask blob,
                       const std::string& claim_id,
                       const std::shared_ptr<ChunkClaim>& owner_state) {
    const auto queue_begin = Clock::now();
    auto retained_credit = std::make_shared<MemoryBudget::Credit>(std::move(task.credit));
    const auto ack_begin = Clock::now();
    const auto promise = task.promise;

    // Transfer payload and credit together; the CPU must not return credit while Storage uses it.
    blob.retained_credit = std::move(retained_credit);

    // Capture short-lived values by value because the callback may run after this function returns.
    // BackupPipeline retains context until workers join, so capturing it by reference is safe.
    blob.completion = [&context, claim_id, owner_state, promise,
                       ack_begin](const WriteAck* ack, std::exception_ptr error) {
        const auto ack_wait =
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - ack_begin).count();
        context.metrics.writer_ack_wait_ns.fetch_add(ack_wait, std::memory_order_relaxed);
        if (error || !ack) {
            const auto failure = error ? error
                                       : std::make_exception_ptr(std::runtime_error(
                                             "object writer returned no acknowledgement"));
            context.claims.fail(claim_id, owner_state, failure);
            try {
                promise->set_exception(failure);
            } catch (...) {
            }
            return;
        }

        // After publication, notify duplicate subscribers before resolving the owner's Metadata result.
        context.claims.complete(claim_id, owner_state, ack->header);
        context.metrics.completed_chunks.fetch_add(1, std::memory_order_relaxed);
        context.metrics.completed_raw_bytes.fetch_add(ack->header.raw_bytes,
                                                      std::memory_order_relaxed);
        try {
            promise->set_value(ack->header);
        } catch (...) {
        }
    };

    // Stage 02 -> 03 handoff waits only for queue capacity, not a durable write acknowledgement.
    const bool queued = context.object_queue.push(std::move(blob), &context.errors.cancelled);
    context.metrics.output_wait_ns.fetch_add(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - queue_begin).count(),
        std::memory_order_relaxed);
    if (!queued)
        throw std::runtime_error("object writer queue closed during backup");
}
} // namespace pbe::detail
