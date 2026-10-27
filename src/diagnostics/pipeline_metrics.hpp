#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace pbe::detail {
// Diagnostic counters updated by workers; never use them to synchronize data access.
// Operation times accumulate across threads and can exceed the run's wall-clock duration.
struct PipelineMetrics {
    // Track computation separately from object verification to avoid counting read waits as CPU work.
    std::atomic<std::uint64_t> cpu_busy_ns{0};
    std::atomic<std::uint64_t> controlled_cpu_ns{0};
    std::atomic<std::uint64_t> hash_ns{0};
    std::atomic<std::uint64_t> compression_ns{0};
    std::atomic<std::uint64_t> decompression_ns{0};
    std::atomic<std::uint64_t> object_verify_ns{0};
    std::atomic<std::uint64_t> object_io_ns{0};

    // Reader scan/read time and Metadata wait/transaction time.
    std::atomic<std::uint64_t> source_scan_ns{0};
    std::atomic<std::uint64_t> source_read_ns{0};
    // Phased mode pauses source reads while draining a batch; this overlaps other stage timings.
    std::atomic<bool> io_draining{false};
    std::atomic<std::uint64_t> io_batches{0};
    std::atomic<std::uint64_t> io_drain_wait_ns{0};
    std::atomic<std::uint64_t> catalog_commit_ns{0};
    std::atomic<std::uint64_t> metadata_ack_wait_ns{0};
    std::atomic<std::uint64_t> metadata_queue_wait_ns{0};
    std::atomic<std::uint64_t> metadata_batches{0};

    // Storage phases: staging writes, file sync, publication, directory sync, and cleanup.
    std::atomic<std::uint64_t> object_setup_ns{0};
    std::atomic<std::uint64_t> object_staging_write_ns{0};
    std::atomic<std::uint64_t> staging_write_calls{0};
    std::atomic<std::uint64_t> object_file_sync_ns{0};
    std::atomic<std::uint64_t> object_publish_ns{0};
    std::atomic<std::uint64_t> object_directory_sync_ns{0};
    std::atomic<std::uint64_t> object_cleanup_ns{0};
    std::atomic<std::uint64_t> object_existing_check_ns{0};
    std::atomic<std::uint64_t> object_write_total_ns{0};
    std::atomic<std::uint64_t> object_write_count{0};
    std::atomic<std::uint64_t> object_write_operation_count{0};

    // Queue and result waits can overlap; their sum is not the backup's wall-clock duration.
    std::atomic<std::uint64_t> output_wait_ns{0};
    std::atomic<std::uint64_t> writer_ack_wait_ns{0};
    std::atomic<std::uint64_t> writer_busy_ns{0};
    std::atomic<std::uint64_t> claim_wait_ns{0};
    std::atomic<std::uint64_t> duplicate_ack_wait_ns{0};

    // Completed work, unique content, and controller overhead.
    std::atomic<std::uint64_t> completed_chunks{0};
    std::atomic<std::uint64_t> completed_raw_bytes{0};
    std::atomic<std::uint64_t> completed_unique_raw_bytes{0};
    std::atomic<std::uint64_t> new_stored_bytes{0};
    std::atomic<std::uint64_t> progress_file_count{0};
    std::atomic<std::uint64_t> worker_churn{0};
    std::atomic<std::uint64_t> controller_overhead_ns{0};
    std::atomic<std::size_t> process_rss_peak_bytes{0};
};

struct ObjectReadTimings;
struct ObjectWriteTimings;
void record_object_read(PipelineMetrics& metrics, const ObjectReadTimings& timings);
void record_object_write(PipelineMetrics& metrics, const ObjectWriteTimings& timings);
std::size_t current_rss_bytes();

} // namespace pbe::detail
