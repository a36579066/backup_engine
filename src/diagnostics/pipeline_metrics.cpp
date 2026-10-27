#include "diagnostics/pipeline_metrics.hpp"
#include "storage/object_store.hpp"

#include <fstream>
#include <unistd.h>
#if defined(__APPLE__)
#include <mach/mach.h>
#include <mach/task_info.h>
#endif

namespace pbe::detail {
void record_object_read(PipelineMetrics& metrics, const ObjectReadTimings& timings) {
    // Verification includes I/O; only decompression and hashing contribute to compute timing.
    metrics.object_verify_ns.fetch_add(timings.total_ns, std::memory_order_relaxed);
    metrics.object_io_ns.fetch_add(timings.io_ns, std::memory_order_relaxed);
    metrics.decompression_ns.fetch_add(timings.decompression_ns, std::memory_order_relaxed);
    metrics.hash_ns.fetch_add(timings.hash_ns, std::memory_order_relaxed);
    metrics.cpu_busy_ns.fetch_add(timings.decompression_ns + timings.hash_ns,
                                  std::memory_order_relaxed);
}

void record_object_write(PipelineMetrics& metrics, const ObjectWriteTimings& timings) {
    metrics.object_setup_ns.fetch_add(timings.setup_ns, std::memory_order_relaxed);
    metrics.object_staging_write_ns.fetch_add(timings.staging_write_ns, std::memory_order_relaxed);
    metrics.staging_write_calls.fetch_add(timings.staging_write_calls, std::memory_order_relaxed);
    metrics.object_file_sync_ns.fetch_add(timings.file_sync_ns, std::memory_order_relaxed);
    metrics.object_publish_ns.fetch_add(timings.publish_ns, std::memory_order_relaxed);
    metrics.object_directory_sync_ns.fetch_add(timings.directory_sync_ns,
                                               std::memory_order_relaxed);
    metrics.object_cleanup_ns.fetch_add(timings.cleanup_ns, std::memory_order_relaxed);
    metrics.object_existing_check_ns.fetch_add(timings.existing_check_ns,
                                               std::memory_order_relaxed);
    metrics.object_write_total_ns.fetch_add(timings.total_ns, std::memory_order_relaxed);
}

std::size_t current_rss_bytes() {
#if defined(__APPLE__)
    mach_task_basic_info_data_t info{};
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info),
                  &count) == KERN_SUCCESS)
        return static_cast<std::size_t>(info.resident_size);
    return 0;
#elif defined(__linux__)
    std::ifstream statm("/proc/self/statm");
    std::uint64_t total_pages = 0, resident_pages = 0;
    if (!(statm >> total_pages >> resident_pages))
        return 0;
    const long page_size = ::sysconf(_SC_PAGESIZE);
    return page_size > 0
               ? static_cast<std::size_t>(resident_pages) * static_cast<std::size_t>(page_size)
               : 0;
#else
    return 0;
#endif
}

} // namespace pbe::detail
