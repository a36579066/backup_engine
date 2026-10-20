#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace pbe {
void note_interrupt() noexcept;
bool interrupt_requested() noexcept;

struct BackupOptions {
    std::filesystem::path repo;
    std::filesystem::path source;
    std::size_t workers = 4;
    std::size_t max_workers = 0;
    std::size_t writer_workers = 1;
    std::size_t memory_mib = 256;
    int zstd_level = 3;
    bool adaptive = false;
    bool skip_unsupported = false;
    bool paranoid = false;
    bool trust_metadata = false;
    std::string failpoint;
    std::size_t test_writer_delay_ms = 0;
    std::size_t test_reader_delay_ms = 0;
    std::size_t test_cpu_spin_ms = 0;
};

struct RestoreOptions {
    std::filesystem::path repo;
    std::filesystem::path target;
    std::string snapshot;
};

struct SnapshotSummary {
    std::string id;
    std::int64_t created_ns = 0;
    std::string source_root;
    std::uint64_t files = 0;
    std::uint64_t directories = 0;
    std::uint64_t symlinks = 0;
    std::uint64_t chunks = 0;
    std::uint64_t logical_bytes = 0;
};

struct BackupSummary : SnapshotSummary {
    std::uint64_t read_bytes = 0;
    std::uint64_t unique_raw_bytes = 0;
    std::uint64_t new_stored_bytes = 0;
    std::uint64_t reused_chunks = 0;
    double elapsed_seconds = 0;
    std::size_t workers = 0;
    std::size_t writer_workers = 1;
    std::string scheduler;
    std::size_t metadata_batches = 0;
    std::string run_id;
    std::size_t memory_credit_peak_bytes = 0;
    std::size_t process_rss_peak_bytes = 0;
    std::size_t worker_churn = 0;
    double controller_overhead_seconds = 0;
};

struct RepositoryStats {
    std::uint64_t snapshots = 0;
    std::uint64_t object_count = 0;
    std::uint64_t referenced_objects = 0;
    std::uint64_t logical_bytes = 0;
    std::uint64_t physical_bytes = 0;
};

void init_repository(const std::filesystem::path& repo, const std::string& storage_format = "files-v1");
BackupSummary backup(const BackupOptions& options);
std::vector<SnapshotSummary> list_snapshots(const std::filesystem::path& repo);
SnapshotSummary show_snapshot(const std::filesystem::path& repo, const std::string& snapshot);
void restore_snapshot(const RestoreOptions& options);
struct VerifySummary { std::uint64_t files = 0, chunks = 0, bytes = 0; };
VerifySummary verify_snapshot(const std::filesystem::path& repo, const std::string& snapshot);
RepositoryStats repository_stats(const std::filesystem::path& repo);
std::string explain_run(const std::filesystem::path& repo, const std::string& run_id, bool json = false);

struct BenchmarkOptions {
    std::filesystem::path dataset;
    std::filesystem::path output = "./pbe-benchmark";
    std::vector<std::size_t> workers{1, 2, 4, 8};
    std::vector<std::string> schedulers{"fixed"};
    std::size_t repeat = 5;
    std::size_t writer_workers = 1;
    std::size_t memory_mib = 256;
    std::string storage_format = "files-v1";
    std::size_t test_writer_delay_ms = 0;
    std::size_t test_reader_delay_ms = 0;
    std::size_t test_cpu_spin_ms = 0;
};
std::string run_benchmark(const BenchmarkOptions& options);
}
