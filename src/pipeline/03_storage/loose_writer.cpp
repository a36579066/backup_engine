#include "pipeline/03_storage/storage_stage.hpp"
#include "pipeline/03_storage/writer_completion.hpp"
#include "pipeline/shared/pipeline_context.hpp"
#include "pipeline/shared/stage_support.hpp"

namespace pbe::detail {
using Clock = PipelineClock;

void run_loose_object_writer(PipelineContext& context) {
    auto& options = context.options;
    auto& repo = context.repo;
    auto& job_id = context.job_id;
    auto& object_queue = context.object_queue;
    auto& errors = context.errors;
    auto& metrics = context.metrics;
    const auto fail = [&context](std::exception_ptr error) {
        context.fail(error);
    };
    try {
        // Report failures for queued tasks on cancellation instead of abandoning their promises.
        while (auto task = object_queue.pop(&errors.cancelled)) {
            if (errors.cancelled.load(std::memory_order_relaxed)) {
                try {
                    complete_blob_task(
                        *task, nullptr,
                        std::make_exception_ptr(std::runtime_error("backup cancelled")));
                } catch (...) {
                }
                continue;
            }
            const auto begin = Clock::now();
            detail::ObjectWriteTimings write_timings;
            bool write_timings_recorded = false;
            try {
                if (!sleep_cancellable(options.test_writer_delay_ms, errors.cancelled)) {
                    throw std::runtime_error("backup cancelled");
                }

                // write_object returns success only after staging, synchronization, and publication.
                const auto ack =
                    write_object(repo, job_id, *task, options.failpoint, &write_timings);
                record_object_write(metrics, write_timings);
                write_timings_recorded = true;
                metrics.object_write_operation_count.fetch_add(1, std::memory_order_relaxed);
                metrics.object_write_count.fetch_add(1, std::memory_order_relaxed);
                metrics.writer_busy_ns.fetch_add(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - begin)
                        .count(),
                    std::memory_order_relaxed);
                if (ack.new_object)
                    metrics.new_stored_bytes.fetch_add(ack.header.stored_bytes + kObjectHeaderSize,
                                                       std::memory_order_relaxed);
                if (ack.new_object)
                    metrics.completed_unique_raw_bytes.fetch_add(ack.header.raw_bytes,
                                                                 std::memory_order_relaxed);

                // Release payload and credit before notifying Metadata that the durable object is available.
                std::vector<std::uint8_t>().swap(task->stored);
                task->retained_credit.reset();
                complete_blob_task(*task, &ack, {});
            } catch (...) {
                const auto failure = std::current_exception();
                if (!write_timings_recorded)
                    record_object_write(metrics, write_timings);
                metrics.writer_busy_ns.fetch_add(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - begin)
                        .count(),
                    std::memory_order_relaxed);
                try {
                    complete_blob_task(*task, nullptr, failure);
                } catch (...) {
                }
                fail(failure);
            }
        }
    } catch (...) {
        fail(std::current_exception());
    }
}
} // namespace pbe::detail
