#include "diagnostics/pipeline_monitor.hpp"
#include "pipeline/shared/pipeline_context.hpp"
#include "pipeline/shared/stage_support.hpp"
#include "scheduler/adaptive_controller.hpp"

#include <algorithm>
#include <chrono>
#include <thread>

namespace pbe::detail {
namespace {
using Clock = PipelineClock;

struct MonitorCounters {
    std::uint64_t cpu = 0;
    std::uint64_t controlled_cpu = 0;
    std::uint64_t hash = 0;
    std::uint64_t compression = 0;
    std::uint64_t decompression = 0;
    std::uint64_t object_verify = 0;
    std::uint64_t object_io = 0;
    std::uint64_t source_scan = 0;
    std::uint64_t source_read = 0;
    std::uint64_t catalog_commit = 0;
    std::uint64_t metadata_ack = 0;
    std::uint64_t metadata_wait = 0;
    std::uint64_t metadata_batches = 0;
    std::uint64_t duplicate_ack = 0;
    std::uint64_t staging_calls = 0;
    std::uint64_t object_setup = 0;
    std::uint64_t object_staging_write = 0;
    std::uint64_t object_file_sync = 0;
    std::uint64_t object_publish = 0;
    std::uint64_t object_directory_sync = 0;
    std::uint64_t object_cleanup = 0;
    std::uint64_t object_existing_check = 0;
    std::uint64_t object_write_total = 0;
    std::uint64_t object_write_count = 0;
    std::uint64_t object_write_operations = 0;
    std::uint64_t output = 0;
    std::uint64_t ack_wait = 0;
    std::uint64_t writer = 0;
    std::uint64_t claim = 0;
    std::uint64_t chunks = 0;
    std::uint64_t bytes = 0;
    std::uint64_t unique_bytes = 0;
    std::uint64_t stored_bytes = 0;
    std::uint64_t progress_files = 0;
    std::uint64_t errors_seen = 0;
    std::uint64_t mem_wait = 0;
    std::uint64_t gate_wait = 0;
};

// Sample in an observer thread without consuming chunks or holding SQLite write transactions.
class PipelineMonitor {
public:
    PipelineMonitor(PipelineContext& owner, const std::atomic<bool>& stop_flag)
        : context(owner),
          stop(stop_flag) {
    }

    void run();

private:
    MonitorCounters sample_counters();
    WindowSample make_window(std::uint64_t elapsed_ns, std::size_t before) const;
    void sample_queues();
    void evaluate_window(std::uint64_t elapsed_ns);

    PipelineContext& context;
    const std::atomic<bool>& stop;
    AdaptiveController controller;
    MonitorCounters previous;
    MonitorCounters current;
    std::uint64_t window_id = 0;
    double input_sum = 0, object_sum = 0, memory_sum = 0, metadata_sum = 0;
    std::uint64_t samples = 0;
};

MonitorCounters PipelineMonitor::sample_counters() {
    MonitorCounters counters;
    counters.cpu = context.metrics.cpu_busy_ns.load(std::memory_order_relaxed);
    counters.controlled_cpu = context.metrics.controlled_cpu_ns.load(std::memory_order_relaxed);
    counters.hash = context.metrics.hash_ns.load(std::memory_order_relaxed);
    counters.compression = context.metrics.compression_ns.load(std::memory_order_relaxed);
    counters.decompression = context.metrics.decompression_ns.load(std::memory_order_relaxed);
    counters.object_verify = context.metrics.object_verify_ns.load(std::memory_order_relaxed);
    counters.object_io = context.metrics.object_io_ns.load(std::memory_order_relaxed);
    counters.source_scan = context.metrics.source_scan_ns.load(std::memory_order_relaxed);
    counters.source_read = context.metrics.source_read_ns.load(std::memory_order_relaxed);
    counters.catalog_commit = context.metrics.catalog_commit_ns.load(std::memory_order_relaxed);
    counters.metadata_ack = context.metrics.metadata_ack_wait_ns.load(std::memory_order_relaxed);
    counters.metadata_wait = context.metrics.metadata_queue_wait_ns.load(std::memory_order_relaxed);
    counters.metadata_batches = context.metrics.metadata_batches.load(std::memory_order_relaxed);
    counters.duplicate_ack = context.metrics.duplicate_ack_wait_ns.load(std::memory_order_relaxed);
    counters.staging_calls = context.metrics.staging_write_calls.load(std::memory_order_relaxed);
    counters.object_setup = context.metrics.object_setup_ns.load(std::memory_order_relaxed);
    counters.object_staging_write =
        context.metrics.object_staging_write_ns.load(std::memory_order_relaxed);
    counters.object_file_sync = context.metrics.object_file_sync_ns.load(std::memory_order_relaxed);
    counters.object_publish = context.metrics.object_publish_ns.load(std::memory_order_relaxed);
    counters.object_directory_sync =
        context.metrics.object_directory_sync_ns.load(std::memory_order_relaxed);
    counters.object_cleanup = context.metrics.object_cleanup_ns.load(std::memory_order_relaxed);
    counters.object_existing_check =
        context.metrics.object_existing_check_ns.load(std::memory_order_relaxed);
    counters.object_write_total =
        context.metrics.object_write_total_ns.load(std::memory_order_relaxed);
    counters.object_write_count =
        context.metrics.object_write_count.load(std::memory_order_relaxed);
    counters.object_write_operations =
        context.metrics.object_write_operation_count.load(std::memory_order_relaxed);
    counters.output = context.metrics.output_wait_ns.load(std::memory_order_relaxed);
    counters.ack_wait = context.metrics.writer_ack_wait_ns.load(std::memory_order_relaxed);
    counters.writer = context.metrics.writer_busy_ns.load(std::memory_order_relaxed);
    counters.claim = context.metrics.claim_wait_ns.load(std::memory_order_relaxed);
    counters.chunks = context.metrics.completed_chunks.load(std::memory_order_relaxed);
    counters.bytes = context.metrics.completed_raw_bytes.load(std::memory_order_relaxed);
    counters.unique_bytes =
        context.metrics.completed_unique_raw_bytes.load(std::memory_order_relaxed);
    counters.stored_bytes = context.metrics.new_stored_bytes.load(std::memory_order_relaxed);
    counters.progress_files = context.metrics.progress_file_count.load(std::memory_order_relaxed);
    counters.errors_seen = context.errors.error_count.load(std::memory_order_relaxed);
    counters.mem_wait = context.memory.wait_ns();
    counters.gate_wait = context.gate.wait_ns();
    return counters;
}

WindowSample PipelineMonitor::make_window(std::uint64_t elapsed_ns, std::size_t before) const {
    // Use counter deltas for window activity and sampled averages for queue levels.
    WindowSample m;
    m.elapsed_ns = elapsed_ns;
    m.controlled_cpu_ns = current.controlled_cpu - previous.controlled_cpu;
    m.hash_ns = current.hash - previous.hash;
    m.compression_ns = current.compression - previous.compression;
    m.decompression_ns = current.decompression - previous.decompression;
    m.object_verify_ns = current.object_verify - previous.object_verify;
    m.object_io_ns = current.object_io - previous.object_io;
    m.source_scan_ns = current.source_scan - previous.source_scan;
    m.source_read_ns = current.source_read - previous.source_read;
    m.catalog_commit_ns = current.catalog_commit - previous.catalog_commit;
    m.metadata_ack_wait_ns = current.metadata_ack - previous.metadata_ack;
    m.metadata_queue_wait_ns = current.metadata_wait - previous.metadata_wait;
    m.metadata_batches = current.metadata_batches - previous.metadata_batches;
    m.duplicate_ack_wait_ns = current.duplicate_ack - previous.duplicate_ack;
    m.staging_write_calls = current.staging_calls - previous.staging_calls;
    m.object_setup_ns = current.object_setup - previous.object_setup;
    m.object_staging_write_ns = current.object_staging_write - previous.object_staging_write;
    m.object_file_sync_ns = current.object_file_sync - previous.object_file_sync;
    m.object_publish_ns = current.object_publish - previous.object_publish;
    m.object_directory_sync_ns = current.object_directory_sync - previous.object_directory_sync;
    m.object_cleanup_ns = current.object_cleanup - previous.object_cleanup;
    m.object_existing_check_ns = current.object_existing_check - previous.object_existing_check;
    m.object_write_total_ns = current.object_write_total - previous.object_write_total;
    m.object_write_count = current.object_write_count - previous.object_write_count;
    m.object_write_operation_count =
        current.object_write_operations - previous.object_write_operations;
    m.completed_chunks = current.chunks - previous.chunks;
    m.completed_bytes = current.bytes - previous.bytes;
    m.unique_raw_bytes = current.unique_bytes - previous.unique_bytes;
    m.committed_object_bytes = current.stored_bytes - previous.stored_bytes;
    m.progress_file_count = current.progress_files - previous.progress_files;
    m.error_count = current.errors_seen - previous.errors_seen;
    m.writer_workers = context.options.writer_workers;
    m.io_strategy = context.options.io_strategy;
    m.io_batch_chunks = context.session.plan.io_policy.batch_chunks;
    m.io_batches_completed = context.metrics.io_batches.load(std::memory_order_relaxed);
    m.io_drain_wait_ns_total = context.metrics.io_drain_wait_ns.load(std::memory_order_relaxed);
    m.io_draining = context.metrics.io_draining.load(std::memory_order_relaxed);
    m.input_queue_fill = samples ? input_sum / samples : 0;
    m.object_queue_fill = samples ? object_sum / samples : 0;
    m.metadata_queue_fill = samples ? metadata_sum / samples : 0;
    m.memory_used_ratio = samples ? memory_sum / samples : 0;
    m.writer_busy_ratio =
        elapsed_ns ? std::clamp(double(current.writer - previous.writer) /
                                    (double(elapsed_ns) * context.options.writer_workers),
                                0.0, 1.0)
                   : 0;

    // Estimate compute occupancy from operation timings, not OS-reported whole-system CPU usage.
    m.cpu_compute_ratio =
        elapsed_ns
            ? std::clamp(double(current.cpu - previous.cpu) / (elapsed_ns * before), 0.0, 1.0)
            : 0;
    m.cpu_admission_wait_ratio = elapsed_ns
                                     ? std::clamp(double(current.gate_wait - previous.gate_wait) /
                                                      (elapsed_ns * context.workers_count),
                                                  0.0, 1.0)
                                     : 0;
    m.output_wait_ratio =
        elapsed_ns
            ? std::clamp(double(current.output - previous.output) / (elapsed_ns * before), 0.0, 1.0)
            : 0;
    m.writer_ack_wait_ratio =
        elapsed_ns
            ? std::clamp(double(current.ack_wait - previous.ack_wait) / (elapsed_ns * before), 0.0,
                         1.0)
            : 0;
    m.claim_wait_ratio = elapsed_ns ? std::clamp(double(current.claim - previous.claim) /
                                                     (elapsed_ns * context.workers_count),
                                                 0.0, 1.0)
                                    : 0;
    m.memory_wait_ratio =
        elapsed_ns ? std::clamp(double(current.mem_wait - previous.mem_wait) / elapsed_ns, 0.0, 1.0)
                   : 0;
    m.throughput_bytes_per_sec = elapsed_ns ? double(m.completed_bytes) * 1e9 / elapsed_ns : 0;
    return m;
}

void PipelineMonitor::sample_queues() {
    input_sum += double(context.input_queue.size()) / context.input_queue.capacity();
    object_sum += double(context.object_queue.size()) / context.object_queue.capacity();
    metadata_sum += double(context.metadata_queue.size()) / context.metadata_queue.capacity();
    memory_sum += double(context.memory.current()) / context.memory.limit();
    const std::size_t rss = current_rss_bytes();
    std::size_t old_rss = context.metrics.process_rss_peak_bytes.load(std::memory_order_relaxed);
    while (rss > old_rss && !context.metrics.process_rss_peak_bytes.compare_exchange_weak(
                                old_rss, rss, std::memory_order_relaxed)) {
    }
    ++samples;
}

void PipelineMonitor::evaluate_window(std::uint64_t elapsed_ns) {
    const auto begin = Clock::now();
    ++window_id;
    current = sample_counters();
    const auto before = context.gate.limit();
    const auto m = make_window(elapsed_ns, before);

    // Classify evidence before adjusting admission; fixed mode still records observations.
    auto decision =
        controller.evaluate(m, classify(m), before, context.workers_count, context.options.adaptive,
                            context.errors.cancelled.load(std::memory_order_relaxed));
    if (decision.limit_changed) {
        context.gate.set_limit(decision.active_limit);
        context.metrics.worker_churn.fetch_add(1, std::memory_order_relaxed);
    }
    std::size_t after = decision.active_limit;
    Diagnosis d = std::move(decision.diagnosis);
    const std::string& action = decision.action;
    const std::string& outcome = decision.outcome;
    if (context.trace) {
        context.trace << make_trace_line(context.run_id, window_id, context.summary.scheduler,
                                         before, after, m, d, action, outcome)
                      << '\n';
        context.trace.flush();
    }
    context.metrics.controller_overhead_ns.fetch_add(detail::elapsed_ns(begin),
                                                     std::memory_order_relaxed);
    previous = current;
    input_sum = object_sum = memory_sum = metadata_sum = 0;
    samples = 0;
}

void PipelineMonitor::run() {
    // Sample queues and memory every 100 ms; evaluate a control window about every two seconds.
    // Record the final partial window so short runs also retain diagnostic observations.
    auto last = Clock::now();
    while (!stop.load(std::memory_order_relaxed) &&
           !context.errors.cancelled.load(std::memory_order_relaxed)) {
        if (interrupt_requested()) {
            context.fail(
                std::make_exception_ptr(std::runtime_error("backup cancelled by interrupt")));
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        sample_queues();
        const auto now = Clock::now();
        const auto elapsed =
            std::chrono::duration_cast<std::chrono::nanoseconds>(now - last).count();
        if (elapsed >= 2000000000ll) {
            evaluate_window(static_cast<std::uint64_t>(elapsed));
            last = now;
        }
    }
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - last).count();
    if (elapsed > 0)
        evaluate_window(static_cast<std::uint64_t>(elapsed));
}

} // namespace

void run_pipeline_monitor(PipelineContext& context, const std::atomic<bool>& stop) {
    PipelineMonitor(context, stop).run();
}
} // namespace pbe::detail
