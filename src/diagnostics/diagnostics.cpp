#include "diagnostics/diagnostics.hpp"

#include <algorithm>
#include <iomanip>
#include <sstream>

namespace pbe::detail {
namespace {
std::string json_escape(std::string_view value) {
    std::ostringstream out;
    for (unsigned char c : value) {
        switch (c) {
        case '"':
            out << "\\\"";
            break;
        case '\\':
            out << "\\\\";
            break;
        case '\b':
            out << "\\b";
            break;
        case '\f':
            out << "\\f";
            break;
        case '\n':
            out << "\\n";
            break;
        case '\r':
            out << "\\r";
            break;
        case '\t':
            out << "\\t";
            break;
        default:
            if (c < 0x20)
                out << "\\u00" << std::hex << std::setw(2) << std::setfill('0') << unsigned(c)
                    << std::dec;
            else
                out << static_cast<char>(c);
        }
    }
    return out.str();
}
} // namespace

Diagnosis classify(const WindowSample& m) {
    // Apply evidence-based rules; reject undersampled windows before classifying waits and backlogs.
    // WRITER_LIMITED identifies a stage bottleneck; it does not prove physical disk saturation.
    if (m.completed_chunks < 4 || m.elapsed_ns < 300000000ull)
        return {};
    if (m.memory_used_ratio >= 0.80 && m.memory_wait_ratio >= 0.15)
        return {"MEMORY_LIMITED", "medium", "MEMORY_CREDITS_R1",
                "Credits were near the configured budget and the reader waited for buffer credits.",
                "A small budget or downstream backpressure can produce similar waits."};
    if (m.writer_busy_ratio >= 0.85 && m.output_wait_ratio >= 0.20 &&
        (m.object_queue_fill >= 0.75 || m.writer_ack_wait_ratio >= 0.20))
        return {
            "WRITER_LIMITED", "medium", "WRITE_BACKPRESSURE_R2",
            "The writer stayed busy while CPU workers were blocked on output queue space.",
            "This points to the writer stage; it does not prove a physical disk bandwidth limit."};
    if (m.claim_wait_ratio >= 0.15)
        return {
            "DEDUP_CONTENTION", "medium", "CLAIM_REGISTRATION_R2",
            "Workers spent a measurable part of the window registering duplicate continuations.",
            "Registry locking and scheduling interleaving can affect registration time; duplicate ACK latency is asynchronous."};
    if (m.cpu_compute_ratio >= 0.65 && m.input_queue_fill >= 0.35 && m.writer_busy_ratio < 0.85 &&
        m.object_queue_fill < 0.75)
        return {
            "CPU_LIMITED", "medium", "CPU_BACKLOG_R1",
            "The input queue held work while compute stages were active and the writer queue had headroom.",
            "Compression level and data compressibility may change CPU demand."};
    if (m.input_queue_fill < 0.10 && m.cpu_compute_ratio < 0.50)
        return {
            "INPUT_LIMITED", "low", "INPUT_STARVATION_R1",
            "Workers had low measured compute activity while the input queue was usually empty.",
            "Filesystem traversal and small-file overhead are plausible input-stage causes."};
    return {"MIXED", "low", "MIXED_EVIDENCE_R1",
            "The measurements do not isolate one dominant pipeline stage.",
            "The input, CPU, memory, and writer stages may all contribute."};
}

std::string make_trace_line(std::string_view run_id, std::uint64_t window_id,
                            std::string_view scheduler, std::size_t before, std::size_t after,
                            const WindowSample& m, const Diagnosis& d, std::string_view action,
                            std::string_view outcome) {
    std::ostringstream out;
    out << std::fixed << std::setprecision(4) << "{\"schema_version\":6,\"run_id\":\""
        << json_escape(run_id) << "\",\"window_id\":" << window_id
        << ",\"elapsed_ms\":" << (m.elapsed_ns / 1000000.0) << ",\"scheduler_mode\":\""
        << json_escape(scheduler) << "\",\"active_limit_before\":" << before
        << ",\"io_strategy\":\"" << json_escape(m.io_strategy) << "\""
        << ",\"active_limit_after\":" << after << ",\"diagnosis\":\""
        << json_escape(d.classification) << "\",\"confidence\":\"" << json_escape(d.confidence)
        << "\",\"observations\":{\"input_queue_fill_ratio\":" << m.input_queue_fill
        << ",\"object_queue_fill_ratio\":" << m.object_queue_fill
        << ",\"metadata_queue_fill_ratio\":" << m.metadata_queue_fill
        << ",\"writer_busy_ratio\":" << m.writer_busy_ratio
        << ",\"writer_workers\":" << m.writer_workers
        << ",\"io_batch_chunks\":" << m.io_batch_chunks
        << ",\"io_batches_completed\":" << m.io_batches_completed
        << ",\"io_drain_wait_ms_total\":" << (m.io_drain_wait_ns_total / 1000000.0)
        << ",\"io_draining\":" << (m.io_draining ? "true" : "false")
        << ",\"cpu_compute_ratio\":" << m.cpu_compute_ratio
        << ",\"controlled_cpu_ms\":" << (m.controlled_cpu_ns / 1000000.0)
        << ",\"hash_ms\":" << (m.hash_ns / 1000000.0)
        << ",\"compression_ms\":" << (m.compression_ns / 1000000.0)
        << ",\"decompression_ms\":" << (m.decompression_ns / 1000000.0)
        << ",\"object_verify_ms\":" << (m.object_verify_ns / 1000000.0)
        << ",\"object_io_ms\":" << (m.object_io_ns / 1000000.0)
        << ",\"source_scan_ms\":" << (m.source_scan_ns / 1000000.0)
        << ",\"source_read_ms\":" << (m.source_read_ns / 1000000.0)
        << ",\"catalog_commit_ms\":" << (m.catalog_commit_ns / 1000000.0)
        << ",\"metadata_ack_wait_ms\":" << (m.metadata_ack_wait_ns / 1000000.0)
        << ",\"metadata_queue_wait_ms\":" << (m.metadata_queue_wait_ns / 1000000.0)
        << ",\"metadata_batches\":" << m.metadata_batches
        << ",\"duplicate_ack_wait_ms\":" << (m.duplicate_ack_wait_ns / 1000000.0)
        << ",\"staging_write_calls\":" << m.staging_write_calls
        << ",\"object_setup_ms\":" << (m.object_setup_ns / 1000000.0)
        << ",\"object_staging_write_ms\":" << (m.object_staging_write_ns / 1000000.0)
        << ",\"object_file_sync_ms\":" << (m.object_file_sync_ns / 1000000.0)
        << ",\"object_publish_ms\":" << (m.object_publish_ns / 1000000.0)
        << ",\"object_directory_sync_ms\":" << (m.object_directory_sync_ns / 1000000.0)
        << ",\"object_cleanup_ms\":" << (m.object_cleanup_ns / 1000000.0)
        << ",\"object_existing_check_ms\":" << (m.object_existing_check_ns / 1000000.0)
        << ",\"object_write_total_ms\":" << (m.object_write_total_ns / 1000000.0)
        << ",\"object_write_count\":" << m.object_write_count
        << ",\"object_write_operation_count\":" << m.object_write_operation_count
        << ",\"cpu_admission_wait_ratio\":" << m.cpu_admission_wait_ratio
        << ",\"cpu_output_wait_ratio\":" << m.output_wait_ratio
        << ",\"writer_ack_wait_ratio\":" << m.writer_ack_wait_ratio
        << ",\"memory_credit_wait_ratio\":" << m.memory_wait_ratio
        << ",\"memory_credit_used_ratio\":" << m.memory_used_ratio
        << ",\"dedup_claim_wait_ratio\":" << m.claim_wait_ratio
        << ",\"completed_chunks\":" << m.completed_chunks
        << ",\"completed_raw_bytes\":" << m.completed_bytes
        << ",\"completed_unique_raw_bytes\":" << m.unique_raw_bytes
        << ",\"committed_object_bytes\":" << m.committed_object_bytes
        << ",\"progress_file_count\":" << m.progress_file_count
        << ",\"error_count\":" << m.error_count
        << ",\"throughput_bytes_per_sec\":" << m.throughput_bytes_per_sec << "}"
        << ",\"rule_id\":\"" << json_escape(d.rule_id) << "\",\"reason\":\""
        << json_escape(d.reason) << "\",\"competing_hypotheses\":\"" << json_escape(d.hypotheses)
        << "\",\"action\":\"" << json_escape(action) << "\",\"outcome\":\"" << json_escape(outcome)
        << "\"}";
    return out.str();
}
} // namespace pbe::detail
