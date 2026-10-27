#include "pipeline/03_storage/storage_stage.hpp"
#include "pipeline/03_storage/writer_completion.hpp"
#include "pipeline/shared/pipeline_context.hpp"
#include "pipeline/shared/stage_support.hpp"

#include <array>
#include <utility>

namespace pbe::detail {
namespace {
constexpr auto kPackIdleFlush = std::chrono::milliseconds(2);

// One segment per Storage worker; reuse vector capacity between packs.
// Retain each pack's tasks until every task receives success or failure notification.
class PackSegment {
public:
    explicit PackSegment(PipelineContext& owner)
        : context(owner) {
        tasks.reserve(kPackMaxObjects);
        acknowledgements.reserve(kPackMaxObjects);
    }

    void write(BlobWriteTask first) {
        tasks.clear();
        acknowledgements.clear();
        test_delay_ns = 0;
        timings = {};
        timings_recorded = false;
        try {
            append_task(std::move(first));

            // Stage objects in batches, then publish the pack and report results together.
            stage_until_flush();
            publish_and_complete();
        } catch (...) {
            const auto error = std::current_exception();
            record_failure();

            // Fail every task if its pack fails so Metadata result waits can terminate.
            for (auto& task : tasks) {
                try {
                    complete_blob_task(task, nullptr, error);
                } catch (...) {
                }
            }
            context.fail(error);
        }
        pack.reset();
    }

private:
    void append_task(BlobWriteTask task) {
        tasks.push_back(std::move(task));
        if (context.errors.cancelled.load(std::memory_order_relaxed))
            throw std::runtime_error("backup cancelled");
        if (!pack) {
            const auto begin = PipelineClock::now();
            if (!sleep_cancellable(context.options.test_writer_delay_ms, context.errors.cancelled))
                throw std::runtime_error("backup cancelled");
            test_delay_ns += elapsed_ns(begin);
            pack = std::make_unique<PackWriter>(context.repo, context.job_id,
                                                context.options.failpoint);
        }
    }

    void stage_available_batch() {
        // Gather only currently available tasks to batch writes without waiting to fill a batch.
        const auto batch_begin = acknowledgements.size();
        while (tasks.size() - batch_begin < kPackWriteBatchObjects &&
               tasks.size() < kPackMaxObjects) {
            auto ready = context.object_queue.pop_until(PipelineClock::now());
            if (!ready)
                break;
            append_task(std::move(*ready));
        }
        std::array<BlobWriteTask*, kPackWriteBatchObjects> batch{};
        const auto size = tasks.size() - batch_begin;
        for (std::size_t i = 0; i < size; ++i)
            batch[i] = &tasks[batch_begin + i];

        // append_batch releases staged payload/credit before the pack's durable ACK is available.
        auto staged = pack->append_batch(std::span<BlobWriteTask*>(batch.data(), size));
        for (auto& ack : staged)
            acknowledgements.push_back(std::move(ack));
    }

    void stage_until_flush() {
        // Publish at object/byte limits or after a brief idle period so tail tasks cannot wait forever.
        for (;;) {
            stage_available_batch();
            if (tasks.size() >= kPackMaxObjects || pack->bytes_written() >= kPackTargetBytes)
                break;
            auto next = context.object_queue.pop_until(PipelineClock::now() + kPackIdleFlush);
            if (!next)
                break;
            append_task(std::move(*next));
        }
    }

    void publish_and_complete() {
        if (context.errors.cancelled.load(std::memory_order_relaxed))
            throw std::runtime_error("backup cancelled");

        // Report success only after file sync, publication, and directory sync complete.
        pack->publish();
        timings = pack->timings();
        record_object_write(context.metrics, timings);
        timings_recorded = true;
        context.metrics.object_write_operation_count.fetch_add(1, std::memory_order_relaxed);
        context.metrics.object_write_count.fetch_add(acknowledgements.size(),
                                                     std::memory_order_relaxed);
        for (const auto& ack : acknowledgements) {
            if (ack.new_object) {
                context.metrics.new_stored_bytes.fetch_add(
                    ack.header.stored_bytes + kObjectHeaderSize, std::memory_order_relaxed);
                context.metrics.completed_unique_raw_bytes.fetch_add(ack.header.raw_bytes,
                                                                     std::memory_order_relaxed);
            }
        }
        context.metrics.writer_busy_ns.fetch_add(timings.total_ns + test_delay_ns,
                                                 std::memory_order_relaxed);

        // Invoke the callback registered by the CPU worker here, on the Writer thread.
        for (std::size_t i = 0; i < tasks.size(); ++i)
            complete_blob_task(tasks[i], &acknowledgements[i], {});
    }

    void record_failure() {
        if (pack) {
            timings = pack->timings();
            if (!timings_recorded)
                record_object_write(context.metrics, timings);
            context.metrics.writer_busy_ns.fetch_add(timings.total_ns + test_delay_ns,
                                                     std::memory_order_relaxed);
        } else {
            context.metrics.writer_busy_ns.fetch_add(test_delay_ns, std::memory_order_relaxed);
        }
    }

    PipelineContext& context;
    std::vector<BlobWriteTask> tasks;
    std::vector<WriteAck> acknowledgements;
    std::unique_ptr<PackWriter> pack;
    ObjectWriteTimings timings;
    std::uint64_t test_delay_ns = 0;
    bool timings_recorded = false;
};
} // namespace

void run_pack_object_writer(PipelineContext& context) {
    try {
        PackSegment segment(context);
        while (auto first = context.object_queue.pop())
            segment.write(std::move(*first));
    } catch (...) {
        context.fail(std::current_exception());
    }
}
} // namespace pbe::detail
