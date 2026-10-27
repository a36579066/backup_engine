#include "core/backup_session.hpp"
#include "pipeline/shared/pipeline_context.hpp"
#include "storage/snapshot_catalog.hpp"

#include <zstd.h>
#include <algorithm>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace pbe::detail {
namespace {
using Clock = std::chrono::steady_clock;

std::int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::string random_id(std::string_view prefix) {
    std::random_device rd;
    std::mt19937_64 rng((std::uint64_t(rd()) << 32) ^ rd() ^
                        static_cast<std::uint64_t>(Clock::now().time_since_epoch().count()));
    std::ostringstream out;
    out << prefix << now_ns() << '-' << std::hex << std::setfill('0') << std::setw(12)
        << (rng() & 0xffffffffffffull);
    return out.str();
}

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

void maybe_failpoint(const std::string& requested, std::string_view name) {
    if (!requested.empty() && requested == name) {
        std::cerr << "PBE fault injection: " << name << "\n";
        std::cerr.flush();
        std::_Exit(86);
    }
}

void insert_snapshot(sqlite3* db, const std::string& id, const std::string& source) {
    Statement statement(
        db,
        "INSERT INTO snapshots(id,created_ns,source_root,status,chunk_bytes) VALUES(?,?,?,'IN_PROGRESS',?)");
    statement.text(1, id);
    statement.integer(2, now_ns());
    statement.text(3, source);
    statement.integer(4, kChunkSize);
    statement.done();
}

void set_snapshot_status(sqlite3* db, const std::string& id, const std::string& status,
                         bool completed) {
    Statement statement(db, completed ? "UPDATE snapshots SET status=?,completed_ns=? WHERE id=?"
                                      : "UPDATE snapshots SET status=? WHERE id=?");
    statement.text(1, status);
    if (completed) {
        statement.integer(2, now_ns());
        statement.text(3, id);
    } else
        statement.text(2, id);
    statement.done();
}

void recover_incomplete(sqlite3* db, const std::filesystem::path& repo) {
    // A previous process may have exited early; unfinished snapshots must never appear COMPLETE.
    Transaction transaction(db);
    exec(db, "UPDATE snapshots SET status='FAILED' WHERE status='IN_PROGRESS'");
    transaction.commit();
    const auto staging = repo / "staging";
    std::error_code ec;
    if (std::filesystem::exists(staging, ec)) {
        for (const auto& entry : std::filesystem::directory_iterator(staging, ec)) {
            if (ec)
                break;
            std::filesystem::remove_all(entry.path(), ec);
            ec.clear();
        }
    }

    // Published packs may lack committed index rows. Remove only packs absent from the catalog.
    if (detail::repository_format_version(repo) == 2) {
        std::set<std::string> referenced_packs;
        Statement packs(db, "SELECT DISTINCT pack_id FROM objects WHERE pack_id IS NOT NULL");
        while (packs.row())
            referenced_packs.insert(packs.string(0));
        const auto pack_dir = repo / "objects" / "packs";
        std::vector<std::filesystem::path> orphan_packs;
        if (std::filesystem::exists(pack_dir, ec)) {
            for (std::filesystem::directory_iterator it(pack_dir, ec), end; !ec && it != end;
                 it.increment(ec)) {
                // AppleDouble sidecars on FAT/exFAT are OS metadata, not repository packs.
                if (!it->path().filename().string().starts_with("pack-") ||
                    it->path().extension() != ".pack")
                    continue;
                const std::string pack_id = it->path().stem().string();
                if (referenced_packs.contains(pack_id))
                    continue;
                const auto status = it->symlink_status(ec);
                if (ec)
                    break;
                if (std::filesystem::is_regular_file(status))
                    orphan_packs.push_back(it->path());
            }
            if (ec)
                throw std::runtime_error("cannot scan object packs during recovery: " +
                                         ec.message());
        }

        // Finish enumeration before deleting: removing a FAT32 file can also remove its
        // AppleDouble sidecar, leaving a cached directory entry that no longer exists.
        bool removed_orphan = false;
        for (const auto& orphan : orphan_packs) {
            if (std::filesystem::remove(orphan, ec))
                removed_orphan = true;
            if (ec)
                throw std::runtime_error("cannot remove orphan object pack: " + ec.message());
        }
        if (removed_orphan)
            fsync_directory(pack_dir);
    }
}

} // namespace

BackupPlan make_backup_plan(const BackupOptions& options) {
    if (options.repo.empty() || options.source.empty())
        throw std::runtime_error("backup requires --repo and --source");
    validate_repo(options.repo);
    const auto repo = canonical_existing(options.repo);
    const bool packed_storage = detail::repository_format_version(repo) == 2;
    const auto source = canonical_existing(options.source);
    if (!std::filesystem::is_directory(source))
        throw std::runtime_error("backup source must be a directory");

    // Reject overlapping source/repository trees to prevent backing the repository up into itself.
    if (path_within(repo, source) || path_within(source, repo))
        throw std::runtime_error("source and repository must not contain one another");
    const std::size_t workers_count =
        options.adaptive ? (options.max_workers ? options.max_workers : options.workers)
                         : options.workers;
    if (workers_count == 0 || workers_count > 256)
        throw std::runtime_error("worker count must be between 1 and 256");
    if (options.writer_workers == 0 || options.writer_workers > 32)
        throw std::runtime_error("writer worker count must be between 1 and 32");
    if (options.zstd_level < ZSTD_minCLevel() || options.zstd_level > ZSTD_maxCLevel())
        throw std::runtime_error("zstd level is outside the supported range");
    if (options.test_writer_delay_ms > 60000 || options.test_reader_delay_ms > 60000 ||
        options.test_cpu_spin_ms > 60000)
        throw std::runtime_error(
            "test stage delays and CPU spin must be at most 60000 milliseconds");
    return BackupPlan{options,        repo,          source,
                      packed_storage, workers_count, make_io_policy(options)};
}

BackupSession::BackupSession(BackupPlan configuration)
    : plan(std::move(configuration)),
      snapshot_id(random_id("bkp-")),
      run_id(random_id("run-")),
      source_text(plan.source.string()),
      job_id(random_id("job-")),
      staging_dir(plan.repo / "staging" / job_id),
      lock_(plan.repo),
      catalog(plan.repo / "catalog.sqlite", true) {
    const auto& options = plan.options;
    const auto workers_count = plan.workers_count;
    summary.id = snapshot_id;
    summary.source_root = source_text;
    summary.scheduler = options.adaptive ? "adaptive" : "fixed";
    summary.workers = workers_count;
    summary.writer_workers = options.writer_workers;
    summary.run_id = run_id;
    summary.io_strategy = options.io_strategy;
    summary.io_batch_chunks = plan.io_policy.batch_chunks;
}

void BackupSession::begin_snapshot() {
    // Commit IN_PROGRESS first so recovery can identify a snapshot interrupted by a process crash.
    recover_incomplete(catalog.get(), plan.repo);
    std::filesystem::create_directories(staging_dir);
    Transaction transaction(catalog.get());
    insert_snapshot(catalog.get(), snapshot_id, source_text);
    transaction.commit();
    started_ = Clock::now();
}

void BackupSession::publish_snapshot() {
    const auto& options = plan.options;
    const auto& repo = plan.repo;

    // Workers have exited; validate chunk layout before committing the COMPLETE status.
    ensure_chunk_layout(catalog.get(), snapshot_id);
    maybe_failpoint(options.failpoint, "before-final-publish");
    {
        Transaction transaction(catalog.get());
        set_snapshot_status(catalog.get(), snapshot_id, "COMPLETE", true);
        transaction.commit();
    }
    maybe_failpoint(options.failpoint, "after-final-publish");
    std::error_code ec;
    std::filesystem::remove_all(staging_dir, ec);
    fsync_directory(repo / "staging");
}

void BackupSession::fail_snapshot() noexcept {
    // Best-effort cleanup must not replace the original backup exception with a secondary error.
    try {
        Transaction transaction(catalog.get());
        set_snapshot_status(catalog.get(), snapshot_id, "FAILED", false);
        transaction.commit();
    } catch (...) {
    }
    std::error_code ec;
    std::filesystem::remove_all(staging_dir, ec);
}

BackupSummary BackupSession::finish_summary(const PipelineContext& pipeline) {
    // Workers have stopped; collect their counters and write this run's diagnostic summary.
    const auto& metrics = pipeline.metrics;
    const auto& memory = pipeline.memory;
    const auto& metadata_queue = pipeline.metadata_queue;
    const auto workers_count = plan.workers_count;
    const auto coordinator_batch_size = pipeline.coordinator_batch_size;
    const auto& repo = plan.repo;
    summary.new_stored_bytes = metrics.new_stored_bytes.load(std::memory_order_relaxed);
    summary.memory_credit_peak_bytes = memory.peak();
    summary.process_rss_peak_bytes = metrics.process_rss_peak_bytes.load(std::memory_order_relaxed);
    summary.worker_churn = metrics.worker_churn.load(std::memory_order_relaxed);
    summary.controller_overhead_seconds =
        metrics.controller_overhead_ns.load(std::memory_order_relaxed) / 1e9;
    summary.metadata_batches = metrics.metadata_batches.load(std::memory_order_relaxed);
    summary.io_batches = metrics.io_batches.load(std::memory_order_relaxed);
    summary.io_drain_wait_seconds = metrics.io_drain_wait_ns.load(std::memory_order_relaxed) / 1e9;
    summary.elapsed_seconds = std::chrono::duration<double>(Clock::now() - started_).count();
    static_cast<SnapshotSummary&>(summary) = query_snapshot(catalog.get(), snapshot_id);
    {
        std::ofstream report(repo / "reports" / (run_id + ".summary.json"));
        if (report)
            report << "{\"schema_version\":1,\"run_id\":\"" << json_escape(run_id)
                   << "\",\"snapshot_id\":\"" << json_escape(snapshot_id) << "\",\"scheduler\":\""
                   << summary.scheduler << "\",\"workers\":" << workers_count
                   << ",\"logical_bytes\":" << summary.logical_bytes << ",\"io_strategy\":\""
                   << json_escape(summary.io_strategy) << "\""
                   << ",\"io_batch_mib\":" << plan.options.io_batch_mib
                   << ",\"io_batch_chunks\":" << summary.io_batch_chunks
                   << ",\"io_batches\":" << summary.io_batches
                   << ",\"io_drain_wait_seconds\":" << summary.io_drain_wait_seconds
                   << ",\"writer_workers\":" << summary.writer_workers << ",\"metadata_batches\":"
                   << metrics.metadata_batches.load(std::memory_order_relaxed)
                   << ",\"staging_write_calls\":"
                   << metrics.staging_write_calls.load(std::memory_order_relaxed)
                   << ",\"metadata_queue_capacity\":" << metadata_queue.capacity()
                   << ",\"metadata_chunk_batch_limit\":" << coordinator_batch_size
                   << ",\"metadata_ack_wait_seconds\":"
                   << metrics.metadata_ack_wait_ns.load(std::memory_order_relaxed) / 1e9
                   << ",\"duplicate_ack_wait_seconds\":"
                   << metrics.duplicate_ack_wait_ns.load(std::memory_order_relaxed) / 1e9
                   << ",\"reader_metadata_queue_wait_seconds\":"
                   << metrics.metadata_queue_wait_ns.load(std::memory_order_relaxed) / 1e9
                   << ",\"read_bytes\":" << summary.read_bytes
                   << ",\"unique_raw_bytes\":" << summary.unique_raw_bytes
                   << ",\"new_stored_bytes\":" << summary.new_stored_bytes
                   << ",\"reused_chunks\":" << summary.reused_chunks
                   << ",\"memory_credit_peak_bytes\":" << memory.peak()
                   << ",\"memory_budget_bytes\":" << memory.limit()
                   << ",\"process_rss_peak_bytes\":" << summary.process_rss_peak_bytes
                   << ",\"worker_churn\":" << summary.worker_churn
                   << ",\"controller_overhead_seconds\":" << summary.controller_overhead_seconds
                   << ",\"elapsed_seconds\":" << summary.elapsed_seconds << "}\n";
    }
    return summary;
}

} // namespace pbe::detail
