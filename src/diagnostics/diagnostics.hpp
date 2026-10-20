#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace pbe::detail {
struct WindowSample {
    std::uint64_t elapsed_ns = 0;
    std::uint64_t controlled_cpu_ns = 0;
    std::uint64_t hash_ns = 0;
    std::uint64_t compression_ns = 0;
    std::uint64_t decompression_ns = 0;
    std::uint64_t object_verify_ns = 0;
    std::uint64_t object_io_ns = 0;
    std::uint64_t source_scan_ns = 0;
    std::uint64_t source_read_ns = 0;
    std::uint64_t catalog_commit_ns = 0;
    std::uint64_t metadata_ack_wait_ns = 0;
    std::uint64_t metadata_queue_wait_ns = 0;
    std::uint64_t metadata_batches = 0;
    std::uint64_t duplicate_ack_wait_ns = 0;
    std::uint64_t staging_write_calls = 0;
    std::uint64_t object_setup_ns = 0;
    std::uint64_t object_staging_write_ns = 0;
    std::uint64_t object_file_sync_ns = 0;
    std::uint64_t object_publish_ns = 0;
    std::uint64_t object_directory_sync_ns = 0;
    std::uint64_t object_cleanup_ns = 0;
    std::uint64_t object_existing_check_ns = 0;
    std::uint64_t object_write_total_ns = 0;
    std::uint64_t object_write_count = 0;
    std::uint64_t object_write_operation_count = 0;
    std::uint64_t completed_chunks = 0;
    std::uint64_t completed_bytes = 0;
    std::uint64_t unique_raw_bytes = 0;
    std::uint64_t committed_object_bytes = 0;
    std::uint64_t progress_file_count = 0;
    std::uint64_t error_count = 0;
    std::size_t writer_workers = 1;
    double input_queue_fill = 0;
    double object_queue_fill = 0;
    double metadata_queue_fill = 0;
    double memory_used_ratio = 0;
    double writer_busy_ratio = 0;
    double cpu_compute_ratio = 0;
    double cpu_admission_wait_ratio = 0;
    double output_wait_ratio = 0;
    double writer_ack_wait_ratio = 0;
    double memory_wait_ratio = 0;
    double claim_wait_ratio = 0;
    double throughput_bytes_per_sec = 0;
};

struct Diagnosis {
    std::string classification = "UNKNOWN";
    std::string confidence = "low";
    std::string rule_id = "INSUFFICIENT_DATA";
    std::string reason = "Not enough work was observed in this control window.";
    std::string hypotheses = "Input, CPU, memory, and writer limits are not distinguishable from this sample.";
};

Diagnosis classify(const WindowSample& sample);
std::string make_trace_line(std::string_view run_id, std::uint64_t window_id,
                            std::string_view scheduler, std::size_t before, std::size_t after,
                            const WindowSample& sample, const Diagnosis& diagnosis,
                            std::string_view action, std::string_view outcome);
} // namespace pbe::detail
