#pragma once

#include "pbe/engine.hpp"
#include "storage/catalog.hpp"
#include "storage/repository.hpp"
#include "scheduler/io_policy.hpp"

#include <chrono>
#include <filesystem>
#include <string>

namespace pbe::detail {
struct PipelineContext;

// Validated settings and absolute paths; reject invalid inputs before starting threads.
struct BackupPlan {
    BackupOptions options;
    std::filesystem::path repo;
    std::filesystem::path source;
    bool packed_storage = false;
    std::size_t workers_count = 0;
    IoPolicy io_policy;
};

BackupPlan make_backup_plan(const BackupOptions& options);

// Own the repository lock and snapshot lifecycle: begin -> pipeline -> publish, or fail.
// Pipeline workers must exit before publishing the snapshot or releasing the catalog connection.
class BackupSession {
public:
    explicit BackupSession(BackupPlan plan);
    void begin_snapshot();
    void publish_snapshot();
    void fail_snapshot() noexcept;
    BackupSummary finish_summary(const PipelineContext& pipeline);

    const BackupPlan plan;
    const std::string snapshot_id;
    const std::string run_id;
    const std::string source_text;
    const std::string job_id;
    const std::filesystem::path staging_dir;

private:
    // Declared before catalog: acquire the lock first and destroy the connection before releasing it.
    RepoLock lock_;

public:
    Catalog catalog;
    BackupSummary summary;

private:
    std::chrono::steady_clock::time_point started_;
};
} // namespace pbe::detail
