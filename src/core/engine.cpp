#include "pbe/engine.hpp"
#include "scheduler/adaptive_controller.hpp"
#include "storage/catalog.hpp"
#include "concurrency/claim_registry.hpp"
#include "concurrency/memory_budget.hpp"
#include "concurrency/worker_pool.hpp"
#include "scheduler/active_worker_gate.hpp"
#include "diagnostics/diagnostics.hpp"
#include "storage/object_store.hpp"
#include "storage/repository.hpp"
#include "storage/snapshot_catalog.hpp"

#include "pbe/blake3.hpp"
#include "pbe/bounded_queue.hpp"

#include <sqlite3.h>
#include <zstd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cctype>
#include <cmath>
#include <chrono>
#include <condition_variable>
#include <ctime>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <csignal>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#if defined(__APPLE__)
#include <mach/mach.h>
#include <mach/task_info.h>
#endif

namespace pbe {
namespace { volatile std::sig_atomic_t g_interrupt_requested = 0; }

void note_interrupt() noexcept { g_interrupt_requested = 1; }
bool interrupt_requested() noexcept { return g_interrupt_requested != 0; }

namespace {
using Clock = std::chrono::steady_clock;
using detail::ActiveWorkerGate;
using detail::Catalog;
using detail::ClaimRegistry;
using detail::MemoryBudget;
using detail::FixedWorkerPool;
using detail::BlobWriteTask;
using detail::ChunkRef;
using detail::DbEntry;
using detail::ObjectHeader;
using detail::ObjectLocation;
using detail::ObjectReadTimings;
using detail::VerifiedObjectCache;
using detail::Statement;
using detail::Transaction;
using detail::WriteAck;
using detail::Diagnosis;
using detail::WindowSample;
using detail::AdaptiveController;
using detail::classify;
using detail::create_schema;
using detail::exec;
using detail::fsync_directory;
using detail::kChunkSize;
using detail::kObjectHeaderSize;
using detail::kPackMaxObjects;
using detail::kPackTargetBytes;
using detail::make_trace_line;
using detail::object_path;
using detail::read_object;
using detail::read_object_header;
using detail::write_object;
using detail::RepoLock;
using detail::canonical_existing;
using detail::ensure_chunk_layout;
using detail::load_chunks;
using detail::load_entries;
using detail::path_within;
using detail::path_parts;
using detail::query_snapshot;
using detail::require_complete;
using detail::validate_repo;

constexpr auto kPackIdleFlush = std::chrono::milliseconds(2);

std::uint64_t elapsed_ns(Clock::time_point start) {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());
}

std::string error_text(std::string_view operation) {
    return std::string(operation) + ": " + std::strerror(errno);
}

std::uint64_t get_le(const std::uint8_t* in, std::size_t bytes) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < bytes; ++i) value |= std::uint64_t(in[i]) << (8 * i);
    return value;
}

std::int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string random_id(std::string_view prefix) {
    std::random_device rd;
    std::mt19937_64 rng((std::uint64_t(rd()) << 32) ^ rd() ^
                        static_cast<std::uint64_t>(Clock::now().time_since_epoch().count()));
    std::ostringstream out;
    out << prefix << now_ns() << '-'
        << std::hex << std::setfill('0') << std::setw(12) << (rng() & 0xffffffffffffull);
    return out.str();
}

std::string json_escape(std::string_view value) {
    std::ostringstream out;
    for (unsigned char c : value) {
        switch (c) {
        case '"': out << "\\\""; break;
        case '\\': out << "\\\\"; break;
        case '\b': out << "\\b"; break;
        case '\f': out << "\\f"; break;
        case '\n': out << "\\n"; break;
        case '\r': out << "\\r"; break;
        case '\t': out << "\\t"; break;
        default:
            if (c < 0x20) out << "\\u00" << std::hex << std::setw(2) << std::setfill('0') << unsigned(c) << std::dec;
            else out << static_cast<char>(c);
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

bool sleep_cancellable(std::size_t milliseconds, const std::atomic<bool>& cancelled) {
    std::size_t remaining = milliseconds;
    while (remaining && !cancelled.load(std::memory_order_relaxed)) {
        const auto step = std::min<std::size_t>(remaining, 50);
        std::this_thread::sleep_for(std::chrono::milliseconds(step));
        remaining -= step;
    }
    return !cancelled.load(std::memory_order_relaxed);
}

std::atomic<std::uint64_t> g_test_cpu_sink{0};
bool spin_cpu_cancellable(std::size_t milliseconds, const std::atomic<bool>& cancelled,
                          std::uint64_t seed) {
    if (milliseconds == 0) return !cancelled.load(std::memory_order_relaxed);
    const auto until = Clock::now() + std::chrono::milliseconds(milliseconds);
    std::uint64_t value = seed | 1u;
    while (Clock::now() < until && !cancelled.load(std::memory_order_relaxed)) {
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        value += 0x9e3779b97f4a7c15ull;
    }
    g_test_cpu_sink.fetch_xor(value, std::memory_order_relaxed);
    return !cancelled.load(std::memory_order_relaxed);
}

struct ChunkTask {
    std::uint64_t ordinal = 0;
    std::vector<std::uint8_t> raw;
    MemoryBudget::Credit credit;
    std::shared_ptr<std::promise<ObjectHeader>> promise;
};

struct PipelineMetrics {
    std::atomic<std::uint64_t> cpu_busy_ns{0};
    std::atomic<std::uint64_t> controlled_cpu_ns{0};
    std::atomic<std::uint64_t> hash_ns{0};
    std::atomic<std::uint64_t> compression_ns{0};
    std::atomic<std::uint64_t> decompression_ns{0};
    std::atomic<std::uint64_t> object_verify_ns{0};
    std::atomic<std::uint64_t> object_io_ns{0};
    std::atomic<std::uint64_t> source_scan_ns{0};
    std::atomic<std::uint64_t> source_read_ns{0};
    std::atomic<std::uint64_t> catalog_commit_ns{0};
    std::atomic<std::uint64_t> object_setup_ns{0};
    std::atomic<std::uint64_t> object_staging_write_ns{0};
    std::atomic<std::uint64_t> object_file_sync_ns{0};
    std::atomic<std::uint64_t> object_publish_ns{0};
    std::atomic<std::uint64_t> object_directory_sync_ns{0};
    std::atomic<std::uint64_t> object_cleanup_ns{0};
    std::atomic<std::uint64_t> object_existing_check_ns{0};
    std::atomic<std::uint64_t> object_write_total_ns{0};
    std::atomic<std::uint64_t> object_write_count{0};
    std::atomic<std::uint64_t> object_write_operation_count{0};
    std::atomic<std::uint64_t> output_wait_ns{0};
    std::atomic<std::uint64_t> writer_ack_wait_ns{0};
    std::atomic<std::uint64_t> writer_busy_ns{0};
    std::atomic<std::uint64_t> claim_wait_ns{0};
    std::atomic<std::uint64_t> completed_chunks{0};
    std::atomic<std::uint64_t> completed_raw_bytes{0};
    std::atomic<std::uint64_t> completed_unique_raw_bytes{0};
    std::atomic<std::uint64_t> new_stored_bytes{0};
    std::atomic<std::uint64_t> progress_file_count{0};
    std::atomic<std::uint64_t> worker_churn{0};
    std::atomic<std::uint64_t> controller_overhead_ns{0};
    std::atomic<std::size_t> process_rss_peak_bytes{0};
};

void record_object_read(PipelineMetrics& metrics, const ObjectReadTimings& timings) {
    metrics.object_verify_ns.fetch_add(timings.total_ns, std::memory_order_relaxed);
    metrics.object_io_ns.fetch_add(timings.io_ns, std::memory_order_relaxed);
    metrics.decompression_ns.fetch_add(timings.decompression_ns, std::memory_order_relaxed);
    metrics.hash_ns.fetch_add(timings.hash_ns, std::memory_order_relaxed);
    metrics.cpu_busy_ns.fetch_add(timings.decompression_ns + timings.hash_ns, std::memory_order_relaxed);
}

void record_object_write(PipelineMetrics& metrics, const detail::ObjectWriteTimings& timings) {
    metrics.object_setup_ns.fetch_add(timings.setup_ns, std::memory_order_relaxed);
    metrics.object_staging_write_ns.fetch_add(timings.staging_write_ns, std::memory_order_relaxed);
    metrics.object_file_sync_ns.fetch_add(timings.file_sync_ns, std::memory_order_relaxed);
    metrics.object_publish_ns.fetch_add(timings.publish_ns, std::memory_order_relaxed);
    metrics.object_directory_sync_ns.fetch_add(timings.directory_sync_ns, std::memory_order_relaxed);
    metrics.object_cleanup_ns.fetch_add(timings.cleanup_ns, std::memory_order_relaxed);
    metrics.object_existing_check_ns.fetch_add(timings.existing_check_ns, std::memory_order_relaxed);
    metrics.object_write_total_ns.fetch_add(timings.total_ns, std::memory_order_relaxed);
}

struct WorkerError {
    std::mutex mutex;
    std::exception_ptr exception;
    std::atomic<bool> cancelled{false};
    std::atomic<std::uint64_t> error_count{0};
    void set(std::exception_ptr value) {
        bool first = false;
        { std::lock_guard lock(mutex); if (!exception) { exception = value; first = true; } }
        if (first) error_count.fetch_add(1, std::memory_order_relaxed);
        cancelled.store(true, std::memory_order_relaxed);
    }
    void rethrow() {
        std::lock_guard lock(mutex);
        if (exception) std::rethrow_exception(exception);
        if (cancelled.load()) throw std::runtime_error("backup cancelled");
    }
};

struct SourceEntry {
    std::filesystem::path absolute;
    std::string relative;
    std::string kind;
    std::uint64_t size = 0;
    std::uint32_t mode = 0;
    std::int64_t mtime_ns = 0;
    std::int64_t ctime_ns = 0;
    std::uint64_t device = 0;
    std::uint64_t inode = 0;
    std::string symlink_target;
};

std::int64_t stat_mtime_ns(const struct stat& st) {
#if defined(__APPLE__)
    return std::int64_t(st.st_mtimespec.tv_sec) * 1000000000ll + st.st_mtimespec.tv_nsec;
#else
    return std::int64_t(st.st_mtim.tv_sec) * 1000000000ll + st.st_mtim.tv_nsec;
#endif
}
std::int64_t stat_ctime_ns(const struct stat& st) {
#if defined(__APPLE__)
    return std::int64_t(st.st_ctimespec.tv_sec) * 1000000000ll + st.st_ctimespec.tv_nsec;
#else
    return std::int64_t(st.st_ctim.tv_sec) * 1000000000ll + st.st_ctim.tv_nsec;
#endif
}

std::vector<SourceEntry> scan_source(const std::filesystem::path& source, bool skip_unsupported,
                                     const std::atomic<bool>& cancelled) {
    std::vector<SourceEntry> entries;
    std::error_code ec;
    std::filesystem::recursive_directory_iterator it(source, std::filesystem::directory_options::none, ec), end;
    if (ec) throw std::runtime_error("cannot scan source: " + ec.message());
    for (; it != end;) {
        if (cancelled.load(std::memory_order_relaxed)) throw std::runtime_error("backup cancelled during source scan");
        if (ec) throw std::runtime_error("source scan failed: " + ec.message());
        const auto path = it->path();
        (void)it->symlink_status(ec);
        if (ec) throw std::runtime_error("cannot stat source entry " + path.string() + ": " + ec.message());
        SourceEntry entry;
        entry.absolute = path;
        entry.relative = path.lexically_relative(source).generic_string();
        if (entry.relative.empty() || entry.relative == "." || entry.relative.find('\0') != std::string::npos || entry.relative.starts_with("../"))
            throw std::runtime_error("source produced an invalid relative path");
        struct stat st{};
        if (::lstat(path.c_str(), &st) != 0) throw std::runtime_error(error_text("stat source entry " + path.string()));
        entry.mode = static_cast<std::uint32_t>(st.st_mode & 07777);
        entry.mtime_ns = stat_mtime_ns(st);
        entry.ctime_ns = stat_ctime_ns(st);
        entry.device = static_cast<std::uint64_t>(st.st_dev);
        entry.inode = static_cast<std::uint64_t>(st.st_ino);
        bool skipped = false;
        if (S_ISDIR(st.st_mode)) { entry.kind = "directory"; }
        else if (S_ISREG(st.st_mode)) { entry.kind = "file"; entry.size = static_cast<std::uint64_t>(st.st_size); }
        else if (S_ISLNK(st.st_mode)) {
            entry.kind = "symlink";
            entry.symlink_target = std::filesystem::read_symlink(path, ec).string();
            if (ec) throw std::runtime_error("cannot read symlink " + path.string() + ": " + ec.message());
        } else {
            if (skip_unsupported) {
                std::cerr << "warning: skipped unsupported source entry: " << path.string() << '\n';
                skipped = true;
            } else {
                throw std::runtime_error("unsupported source entry: " + path.string());
            }
        }
        if (!skipped) entries.push_back(std::move(entry));
        it.increment(ec);
        if (ec) throw std::runtime_error("source scan failed: " + ec.message());
    }
    std::sort(entries.begin(), entries.end(), [](const auto& a, const auto& b) { return a.relative < b.relative; });
    return entries;
}

void insert_snapshot(sqlite3* db, const std::string& id, const std::string& source) {
    Statement statement(db, "INSERT INTO snapshots(id,created_ns,source_root,status,chunk_bytes) VALUES(?,?,?,'IN_PROGRESS',?)");
    statement.text(1, id); statement.integer(2, now_ns()); statement.text(3, source); statement.integer(4, kChunkSize); statement.done();
}

void set_snapshot_status(sqlite3* db, const std::string& id, const std::string& status, bool completed) {
    Statement statement(db, completed ? "UPDATE snapshots SET status=?,completed_ns=? WHERE id=?" : "UPDATE snapshots SET status=? WHERE id=?");
    statement.text(1, status);
    if (completed) { statement.integer(2, now_ns()); statement.text(3, id); }
    else statement.text(2, id);
    statement.done();
}

void insert_entry(sqlite3* db, const std::string& snapshot, const SourceEntry& entry) {
    Statement statement(db, "INSERT INTO entries(snapshot_id,relative_path,kind,size_bytes,mode_bits,mtime_ns,symlink_target) VALUES(?,?,?,?,?,?,?)");
    statement.text(1, snapshot); statement.text(2, entry.relative); statement.text(3, entry.kind);
    statement.integer(4, static_cast<std::int64_t>(entry.size)); statement.integer(5, entry.mode); statement.integer(6, entry.mtime_ns);
    if (entry.kind == "symlink") statement.text(7, entry.symlink_target); else statement.null(7);
    statement.done();
}

void insert_object(sqlite3* db, const std::string& id, const ObjectHeader& header) {
    if (header.pack_id.empty()) {
        Statement statement(db, "INSERT INTO objects(object_id,raw_bytes,stored_bytes,codec) VALUES(?,?,?,?) ON CONFLICT(object_id) DO UPDATE SET raw_bytes=excluded.raw_bytes,stored_bytes=excluded.stored_bytes,codec=excluded.codec");
        statement.text(1, id); statement.integer(2, static_cast<std::int64_t>(header.raw_bytes));
        statement.integer(3, static_cast<std::int64_t>(header.stored_bytes)); statement.text(4, header.codec ? "zstd" : "raw"); statement.done();
    } else {
        Statement statement(db, "INSERT INTO objects(object_id,raw_bytes,stored_bytes,codec,pack_id,pack_offset) VALUES(?,?,?,?,?,?) ON CONFLICT(object_id) DO UPDATE SET raw_bytes=excluded.raw_bytes,stored_bytes=excluded.stored_bytes,codec=excluded.codec,pack_id=excluded.pack_id,pack_offset=excluded.pack_offset");
        statement.text(1, id); statement.integer(2, static_cast<std::int64_t>(header.raw_bytes));
        statement.integer(3, static_cast<std::int64_t>(header.stored_bytes)); statement.text(4, header.codec ? "zstd" : "raw");
        statement.text(5, header.pack_id); statement.integer(6, static_cast<std::int64_t>(header.pack_offset)); statement.done();
    }
}

void insert_chunk(sqlite3* db, const std::string& snapshot, const std::string& path,
                  std::uint64_t ordinal, const std::string& id, std::uint64_t bytes) {
    Statement statement(db, "INSERT INTO entry_chunks(snapshot_id,relative_path,ordinal,object_id,chunk_bytes) VALUES(?,?,?,?,?)");
    statement.text(1, snapshot); statement.text(2, path); statement.integer(3, static_cast<std::int64_t>(ordinal));
    statement.text(4, id); statement.integer(5, static_cast<std::int64_t>(bytes)); statement.done();
}

void recover_incomplete(sqlite3* db, const std::filesystem::path& repo) {
    Transaction transaction(db);
    exec(db, "UPDATE snapshots SET status='FAILED' WHERE status='IN_PROGRESS'");
    transaction.commit();
    const auto staging = repo / "staging";
    std::error_code ec;
    if (std::filesystem::exists(staging, ec)) {
        for (const auto& entry : std::filesystem::directory_iterator(staging, ec)) {
            if (ec) break;
            std::filesystem::remove_all(entry.path(), ec);
            ec.clear();
        }
    }
    if (detail::repository_format_version(repo) == 2) {
        std::set<std::string> referenced_packs;
        Statement packs(db, "SELECT DISTINCT pack_id FROM objects WHERE pack_id IS NOT NULL");
        while (packs.row()) referenced_packs.insert(packs.string(0));
        const auto pack_dir = repo / "objects" / "packs";
        bool removed_orphan = false;
        if (std::filesystem::exists(pack_dir, ec)) {
            for (std::filesystem::directory_iterator it(pack_dir, ec), end; !ec && it != end; it.increment(ec)) {
                const auto status = it->symlink_status(ec);
                if (ec) break;
                if (!std::filesystem::is_regular_file(status) || it->path().extension() != ".pack") continue;
                const std::string pack_id = it->path().stem().string();
                if (referenced_packs.contains(pack_id)) continue;
                if (std::filesystem::remove(it->path(), ec)) removed_orphan = true;
                if (ec) break;
            }
            if (ec) throw std::runtime_error("cannot scan object packs during recovery: " + ec.message());
        }
        if (removed_orphan) fsync_directory(pack_dir);
    }
}

void insert_chunk_batch(sqlite3* db, const std::string& snapshot, const std::string& path,
                        const std::vector<ChunkRef>& chunks, const std::vector<ObjectHeader>& headers,
                        const std::string& failpoint) {
    if (chunks.size() != headers.size()) throw std::runtime_error("chunk metadata batch is inconsistent");
    Transaction transaction(db);
    for (std::size_t i = 0; i < chunks.size(); ++i) {
        insert_object(db, chunks[i].object_id, headers[i]);
        insert_chunk(db, snapshot, path, chunks[i].ordinal, chunks[i].object_id, chunks[i].bytes);
    }
    maybe_failpoint(failpoint, "before-sqlite-batch-commit");
    transaction.commit();
}

std::size_t current_rss_bytes() {
#if defined(__APPLE__)
    mach_task_basic_info_data_t info{};
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &count) == KERN_SUCCESS)
        return static_cast<std::size_t>(info.resident_size);
    return 0;
#elif defined(__linux__)
    std::ifstream statm("/proc/self/statm");
    std::uint64_t total_pages = 0, resident_pages = 0;
    if (!(statm >> total_pages >> resident_pages)) return 0;
    const long page_size = ::sysconf(_SC_PAGESIZE);
    return page_size > 0 ? static_cast<std::size_t>(resident_pages) * static_cast<std::size_t>(page_size) : 0;
#else
    return 0;
#endif
}

struct CachedEntry {
    std::uint64_t size = 0;
    std::uint32_t mode = 0;
    std::int64_t mtime_ns = 0;
    std::string kind;
    std::string symlink_target;
};

std::unordered_map<std::string, CachedEntry> load_metadata_cache(sqlite3* db, const std::string& source,
                                                                  std::string& snapshot_id) {
    Statement previous(db, "SELECT id FROM snapshots WHERE source_root=? AND status='COMPLETE' ORDER BY created_ns DESC LIMIT 1");
    previous.text(1, source);
    if (!previous.row()) return {};
    snapshot_id = previous.string(0);
    Statement entries(db, "SELECT relative_path,kind,size_bytes,mode_bits,mtime_ns,COALESCE(symlink_target,'') FROM entries WHERE snapshot_id=?");
    entries.text(1, snapshot_id);
    std::unordered_map<std::string, CachedEntry> cache;
    while (entries.row()) cache.emplace(entries.string(0), CachedEntry{
        static_cast<std::uint64_t>(entries.i64(2)), static_cast<std::uint32_t>(entries.i64(3)),
        entries.i64(4), entries.string(1), entries.string(5)});
    return cache;
}

void snapshot_entry_transaction(sqlite3* db, const std::string& snapshot,
                                const SourceEntry& entry, const std::string& failpoint) {
    Transaction transaction(db);
    insert_entry(db, snapshot, entry);
    maybe_failpoint(failpoint, "before-sqlite-batch-commit");
    transaction.commit();
}

} // namespace

void init_repository(const std::filesystem::path& input, const std::string& storage_format) {
    if (input.empty()) throw std::runtime_error("--repo is required");
    if (storage_format != "files-v1" && storage_format != "pack-v2")
        throw std::runtime_error("storage format must be files-v1 or pack-v2");
    std::error_code ec;
    auto repo = std::filesystem::absolute(input, ec).lexically_normal();
    if (ec) throw std::runtime_error("cannot resolve repository path: " + ec.message());
    bool created = false;
    if (!std::filesystem::exists(repo, ec)) {
        if (!std::filesystem::create_directories(repo, ec) || ec) throw std::runtime_error("cannot create repository directory: " + ec.message());
        created = true;
    }
    if (!std::filesystem::is_directory(repo)) throw std::runtime_error("repository path is not a directory");
    if (!created && std::filesystem::directory_iterator(repo) != std::filesystem::directory_iterator{})
        throw std::runtime_error("refusing to initialize a non-empty directory");
    std::filesystem::create_directories(repo / "objects");
    if (storage_format == "pack-v2") std::filesystem::create_directories(repo / "objects" / "packs");
    std::filesystem::create_directories(repo / "staging");
    std::filesystem::create_directories(repo / "reports");
    {
        std::ofstream config(repo / "repo.toml", std::ios::binary | std::ios::out | std::ios::trunc);
        if (!config) throw std::runtime_error("cannot create repository config");
        config << "format_version = " << (storage_format == "pack-v2" ? 2 : 1)
               << "\nchunk_size = 1048576\nhash_algorithm = \"blake3-256\"\nobject_format = 1\nstorage_format = \""
               << storage_format << "\"\n";
        config.flush();
        if (!config) throw std::runtime_error("cannot write repository config");
    }
    int config_fd = ::open((repo / "repo.toml").c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (config_fd < 0) throw std::runtime_error(error_text("open repository config for sync"));
    if (::fsync(config_fd) != 0) { const auto e = error_text("sync repository config"); ::close(config_fd); throw std::runtime_error(e); }
    ::close(config_fd);
    {
        Catalog catalog(repo / "catalog.sqlite", true);
        create_schema(catalog.get());
    }
    if (storage_format == "pack-v2") {
        fsync_directory(repo / "objects" / "packs");
        fsync_directory(repo / "objects");
    }
    int lockfd = ::open((repo / ".repo.lock").c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
    if (lockfd >= 0) { ::fsync(lockfd); ::close(lockfd); }
    fsync_directory(repo);
}

BackupSummary backup(const BackupOptions& options) {
    if (options.repo.empty() || options.source.empty()) throw std::runtime_error("backup requires --repo and --source");
    validate_repo(options.repo);
    const auto repo = canonical_existing(options.repo);
    const bool packed_storage = detail::repository_format_version(repo) == 2;
    const auto source = canonical_existing(options.source);
    if (!std::filesystem::is_directory(source)) throw std::runtime_error("backup source must be a directory");
    if (path_within(repo, source) || path_within(source, repo))
        throw std::runtime_error("source and repository must not contain one another");
    const std::size_t workers_count = options.adaptive
        ? (options.max_workers ? options.max_workers : options.workers) : options.workers;
    if (workers_count == 0 || workers_count > 256) throw std::runtime_error("worker count must be between 1 and 256");
    if (options.writer_workers == 0 || options.writer_workers > 32)
        throw std::runtime_error("writer worker count must be between 1 and 32");
    if (options.memory_mib < 3 || options.memory_mib > (std::numeric_limits<std::size_t>::max() >> 20))
        throw std::runtime_error("--memory-mib must be at least 3 and fit in this process");
    if (options.zstd_level < ZSTD_minCLevel() || options.zstd_level > ZSTD_maxCLevel())
        throw std::runtime_error("zstd level is outside the supported range");
    if (options.test_writer_delay_ms > 60000 || options.test_reader_delay_ms > 60000 || options.test_cpu_spin_ms > 60000)
        throw std::runtime_error("test stage delays and CPU spin must be at most 60000 milliseconds");
    RepoLock lock(repo);
    Catalog catalog(repo / "catalog.sqlite", true);
    recover_incomplete(catalog.get(), repo);
    const std::string snapshot_id = random_id("bkp-");
    const std::string run_id = random_id("run-");
    const std::string source_text = source.string();
    const std::string job_id = random_id("job-");
    const auto staging_dir = repo / "staging" / job_id;
    std::filesystem::create_directories(staging_dir);
    {
        Transaction transaction(catalog.get());
        insert_snapshot(catalog.get(), snapshot_id, source_text);
        transaction.commit();
    }

    BackupSummary summary;
    summary.id = snapshot_id;
    summary.source_root = source_text;
    summary.scheduler = options.adaptive ? "adaptive" : "fixed";
    summary.workers = workers_count;
    summary.writer_workers = options.writer_workers;
    summary.run_id = run_id;
    const auto start = Clock::now();
    const std::size_t buffer_budget = options.memory_mib * (std::size_t(1) << 20);
    const std::size_t credit_per_chunk = kChunkSize + ZSTD_compressBound(kChunkSize);
    const std::size_t coordinator_batch_size = packed_storage
        ? kPackMaxObjects * options.writer_workers
        : std::max<std::size_t>(8, workers_count * 4);
    MemoryBudget memory(buffer_budget);
    ActiveWorkerGate gate(workers_count, options.adaptive ? 1 : workers_count);
    const std::size_t input_queue_capacity = std::max<std::size_t>(2, workers_count * 2);
    const std::size_t object_queue_capacity = packed_storage
        ? std::max(input_queue_capacity, coordinator_batch_size)
        : input_queue_capacity;
    BoundedQueue<ChunkTask> input_queue(input_queue_capacity);
    BoundedQueue<BlobWriteTask> object_queue(object_queue_capacity);
    ClaimRegistry<ObjectHeader> claims;
    VerifiedObjectCache verified_existing_objects(4096);
    WorkerError errors;
    PipelineMetrics metrics;
    metrics.process_rss_peak_bytes.store(current_rss_bytes(), std::memory_order_relaxed);
    FixedWorkerPool cpu_workers;
    FixedWorkerPool blob_writers;
    std::vector<std::unique_ptr<Catalog>> object_index_readers;
    std::thread monitor;
    std::atomic<bool> monitor_stop{false};
    bool monitor_started = false;
    auto fail = [&](std::exception_ptr error) {
        errors.set(error);
        input_queue.close();
        object_queue.close();
        gate.close();
    };
    auto finish_threads = [&](bool failed) {
        if (failed) {
            input_queue.close(); object_queue.close(); gate.close();
        } else {
            input_queue.close();
        }
        cpu_workers.join();
        object_queue.close();
        blob_writers.join();
        if (monitor_started) { monitor_stop.store(true, std::memory_order_relaxed); if (monitor.joinable()) monitor.join(); monitor_started = false; }
    };

    std::ofstream trace(repo / "reports" / (run_id + ".jsonl"), std::ios::out | std::ios::app);
    if (!trace) std::cerr << "warning: decision trace could not be opened; backup correctness is unaffected\n";

    try {
        if (packed_storage) {
            object_index_readers.reserve(workers_count);
            for (std::size_t i = 0; i < workers_count; ++i)
                object_index_readers.push_back(std::make_unique<Catalog>(repo / "catalog.sqlite"));
        }
        if (packed_storage) {
            blob_writers.start(options.writer_workers, [&](std::size_t) {
                std::vector<BlobWriteTask> segment;
                std::vector<WriteAck> acknowledgements;
                segment.reserve(kPackMaxObjects);
                acknowledgements.reserve(kPackMaxObjects);
                try {
                    while (auto first = object_queue.pop()) {
                        segment.clear();
                        acknowledgements.clear();
                        std::unique_ptr<detail::PackWriter> pack;
                        std::uint64_t test_delay_ns = 0;
                        detail::ObjectWriteTimings write_timings;
                        bool timings_recorded = false;
                        const auto append_task = [&](BlobWriteTask&& task) {
                            segment.push_back(std::move(task));
                            if (errors.cancelled.load(std::memory_order_relaxed))
                                throw std::runtime_error("backup cancelled");
                            if (!pack) {
                                const auto delay_begin = Clock::now();
                                if (!sleep_cancellable(options.test_writer_delay_ms, errors.cancelled))
                                    throw std::runtime_error("backup cancelled");
                                test_delay_ns += elapsed_ns(delay_begin);
                                pack = std::make_unique<detail::PackWriter>(repo, job_id, options.failpoint);
                            }
                            acknowledgements.push_back(pack->append(segment.back()));
                        };
                        try {
                            append_task(std::move(*first));
                            while (segment.size() < kPackMaxObjects && pack->bytes_written() < kPackTargetBytes) {
                                auto next = object_queue.pop_until(std::chrono::steady_clock::now() + kPackIdleFlush);
                                if (!next) break;
                                append_task(std::move(*next));
                            }
                            if (errors.cancelled.load(std::memory_order_relaxed))
                                throw std::runtime_error("backup cancelled");
                            pack->publish();
                            write_timings = pack->timings();
                            record_object_write(metrics, write_timings);
                            timings_recorded = true;
                            metrics.object_write_operation_count.fetch_add(1, std::memory_order_relaxed);
                            metrics.object_write_count.fetch_add(acknowledgements.size(), std::memory_order_relaxed);
                            for (const auto& ack : acknowledgements) {
                                if (ack.new_object) metrics.new_stored_bytes.fetch_add(ack.header.stored_bytes + kObjectHeaderSize, std::memory_order_relaxed);
                                if (ack.new_object) metrics.completed_unique_raw_bytes.fetch_add(ack.header.raw_bytes, std::memory_order_relaxed);
                            }
                            metrics.writer_busy_ns.fetch_add(write_timings.total_ns + test_delay_ns, std::memory_order_relaxed);
                            for (std::size_t i = 0; i < segment.size(); ++i) {
                                if (segment[i].completion) segment[i].completion(&acknowledgements[i], {});
                                else if (segment[i].promise) segment[i].promise->set_value(acknowledgements[i]);
                            }
                        } catch (...) {
                            const auto error = std::current_exception();
                            if (pack) {
                                write_timings = pack->timings();
                                if (!timings_recorded) record_object_write(metrics, write_timings);
                                metrics.writer_busy_ns.fetch_add(write_timings.total_ns + test_delay_ns, std::memory_order_relaxed);
                            } else {
                                metrics.writer_busy_ns.fetch_add(test_delay_ns, std::memory_order_relaxed);
                            }
                            for (auto& task : segment) {
                                try {
                                    if (task.completion) task.completion(nullptr, error);
                                    else if (task.promise) task.promise->set_exception(error);
                                } catch (...) {}
                            }
                            fail(error);
                        }
                    }
                } catch (...) { fail(std::current_exception()); }
            });
        } else {
            blob_writers.start(options.writer_workers, [&](std::size_t) {
                try {
                    while (auto task = object_queue.pop(&errors.cancelled)) {
                        if (errors.cancelled.load(std::memory_order_relaxed)) {
                            try { task->promise->set_exception(std::make_exception_ptr(std::runtime_error("backup cancelled"))); } catch (...) {}
                            continue;
                        }
                        const auto begin = Clock::now();
                        detail::ObjectWriteTimings write_timings;
                        bool write_timings_recorded = false;
                        try {
                            if (!sleep_cancellable(options.test_writer_delay_ms, errors.cancelled)) {
                                task->promise->set_exception(std::make_exception_ptr(std::runtime_error("backup cancelled")));
                                continue;
                            }
                            const auto ack = write_object(repo, job_id, *task, options.failpoint, &write_timings);
                            record_object_write(metrics, write_timings);
                            write_timings_recorded = true;
                            metrics.object_write_operation_count.fetch_add(1, std::memory_order_relaxed);
                            metrics.object_write_count.fetch_add(1, std::memory_order_relaxed);
                            metrics.writer_busy_ns.fetch_add(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - begin).count(), std::memory_order_relaxed);
                            if (ack.new_object) metrics.new_stored_bytes.fetch_add(ack.header.stored_bytes + kObjectHeaderSize, std::memory_order_relaxed);
                            if (ack.new_object) metrics.completed_unique_raw_bytes.fetch_add(ack.header.raw_bytes, std::memory_order_relaxed);
                            task->promise->set_value(ack);
                        } catch (...) {
                            if (!write_timings_recorded) record_object_write(metrics, write_timings);
                            metrics.writer_busy_ns.fetch_add(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - begin).count(), std::memory_order_relaxed);
                            try { task->promise->set_exception(std::current_exception()); } catch (...) {}
                            fail(std::current_exception());
                        }
                    }
                } catch (...) { fail(std::current_exception()); }
            });
        }

        cpu_workers.start(workers_count, [&](std::size_t i) {
                sqlite3* worker_catalog = packed_storage ? object_index_readers.at(i)->get() : nullptr;
                while (auto item = input_queue.pop(&errors.cancelled)) {
                    ChunkTask task = std::move(*item);
                    std::shared_ptr<ClaimRegistry<ObjectHeader>::State> owner_state;
                    std::string claim_id;
                    bool is_owner = false;
                    bool permit = false;
                    try {
                        if (errors.cancelled.load(std::memory_order_relaxed)) throw std::runtime_error("backup cancelled");
                        if (!gate.acquire(errors.cancelled)) throw std::runtime_error("backup cancelled while waiting for CPU admission");
                        permit = true;
                        const auto hash_begin = Clock::now();
                        const Digest digest = blake3(task.raw);
                        const auto hash_ns = elapsed_ns(hash_begin);
                        metrics.hash_ns.fetch_add(hash_ns, std::memory_order_relaxed);
                        metrics.cpu_busy_ns.fetch_add(hash_ns, std::memory_order_relaxed);
                        const std::uint64_t digest_seed = get_le(digest.data(), 8);
                        const auto spin_begin = Clock::now();
                        const bool spin_ok = spin_cpu_cancellable(options.test_cpu_spin_ms, errors.cancelled, digest_seed);
                        const auto spin_ns = elapsed_ns(spin_begin);
                        metrics.controlled_cpu_ns.fetch_add(spin_ns, std::memory_order_relaxed);
                        metrics.cpu_busy_ns.fetch_add(spin_ns, std::memory_order_relaxed);
                        if (!spin_ok)
                            throw std::runtime_error("backup cancelled during controlled CPU work");
                        const std::string id = digest_hex(digest);
                        ObjectHeader existing_header;
                        ObjectLocation existing_location;
                        std::filesystem::path stored_path;
                        bool object_exists = false;
                        if (packed_storage) {
                            auto location = detail::lookup_object_location(worker_catalog, id);
                            if (location) {
                                existing_location = *location;
                                const auto header_begin = Clock::now();
                                existing_header = detail::read_packed_object_header(repo, existing_location);
                                metrics.object_io_ns.fetch_add(elapsed_ns(header_begin), std::memory_order_relaxed);
                                object_exists = true;
                            }
                        } else {
                            stored_path = object_path(repo, id);
                            if (std::filesystem::exists(stored_path)) {
                                const auto header_begin = Clock::now();
                                existing_header = read_object_header(stored_path);
                                metrics.object_io_ns.fetch_add(elapsed_ns(header_begin), std::memory_order_relaxed);
                                object_exists = true;
                            }
                        }
                        if (object_exists) {
                            const std::size_t raw_size = task.raw.size();
                            if (existing_header.digest != digest || existing_header.raw_bytes != raw_size) throw std::runtime_error("existing object conflicts with chunk digest");
                            const bool verify_existing = !verified_existing_objects.contains_or_insert(id);
                            // The reserved credit covers the stored payload and decoded bytes once
                            // this source buffer is released.
                            std::vector<std::uint8_t>().swap(task.raw);
                            if (verify_existing) {
                                ObjectReadTimings timings;
                                const auto verified_raw = packed_storage
                                    ? detail::read_packed_object(repo, existing_location, id, &timings)
                                    : read_object(stored_path, id, &timings);
                                if (verified_raw.size() != raw_size) throw std::runtime_error("existing object content length differs from its digest");
                                record_object_read(metrics, timings);
                            }
                            gate.release(); permit = false;
                            metrics.completed_chunks.fetch_add(1, std::memory_order_relaxed);
                            metrics.completed_raw_bytes.fetch_add(raw_size, std::memory_order_relaxed);
                            existing_header.newly_written = false;
                            task.promise->set_value(existing_header);
                            continue;
                        }
                        claim_id = id;
                        auto [state, owner] = claims.claim(id);
                        is_owner = owner;
                        owner_state = state;
                        if (!owner) {
                            gate.release(); permit = false;
                            const auto wait_begin = Clock::now();
                            ObjectHeader header = claims.wait(state, errors.cancelled);
                            metrics.claim_wait_ns.fetch_add(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - wait_begin).count(), std::memory_order_relaxed);
                            header.newly_written = false;
                            metrics.completed_chunks.fetch_add(1, std::memory_order_relaxed);
                            metrics.completed_raw_bytes.fetch_add(task.raw.size(), std::memory_order_relaxed);
                            task.promise->set_value(header);
                            continue;
                        }

                        std::vector<std::uint8_t> compressed(ZSTD_compressBound(task.raw.size()));
                        const auto compression_begin = Clock::now();
                        const std::size_t compressed_size = ZSTD_compress(compressed.data(), compressed.size(),
                            task.raw.data(), task.raw.size(), options.zstd_level);
                        const auto compression_ns = elapsed_ns(compression_begin);
                        metrics.compression_ns.fetch_add(compression_ns, std::memory_order_relaxed);
                        metrics.cpu_busy_ns.fetch_add(compression_ns, std::memory_order_relaxed);
                        if (ZSTD_isError(compressed_size)) throw std::runtime_error(std::string("zstd compression failed: ") + ZSTD_getErrorName(compressed_size));
                        BlobWriteTask blob;
                        blob.digest = digest;
                        blob.raw_bytes = task.raw.size();
                        blob.promise = std::make_shared<std::promise<WriteAck>>();
                        auto ack_future = blob.promise->get_future();
                        if (compressed_size < task.raw.size()) {
                            compressed.resize(compressed_size);
                            blob.codec = 1;
                            blob.stored = std::move(compressed);
                        } else {
                            blob.codec = 0;
                            blob.stored = std::move(task.raw);
                        }
                        gate.release(); permit = false;
                        const auto queue_begin = Clock::now();
                        if (packed_storage) {
                            auto retained_credit = std::make_shared<MemoryBudget::Credit>(std::move(task.credit));
                            const auto ack_begin = Clock::now();
                            const auto promise = task.promise;
                            blob.retained_credit = std::move(retained_credit);
                            blob.completion = [&, claim_id, owner_state, promise, ack_begin](
                                const WriteAck* ack, std::exception_ptr error) {
                                const auto ack_wait = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - ack_begin).count();
                                metrics.writer_ack_wait_ns.fetch_add(ack_wait, std::memory_order_relaxed);
                                metrics.output_wait_ns.fetch_add(ack_wait, std::memory_order_relaxed);
                                if (error || !ack) {
                                    const auto failure = error ? error : std::make_exception_ptr(
                                        std::runtime_error("pack writer returned no acknowledgement"));
                                    claims.fail(claim_id, owner_state, failure);
                                    try { promise->set_exception(failure); } catch (...) {}
                                    return;
                                }
                                claims.complete(claim_id, owner_state, ack->header);
                                metrics.completed_chunks.fetch_add(1, std::memory_order_relaxed);
                                metrics.completed_raw_bytes.fetch_add(ack->header.raw_bytes, std::memory_order_relaxed);
                                try { promise->set_value(ack->header); } catch (...) {}
                            };
                            const bool queued = object_queue.push(std::move(blob), &errors.cancelled);
                            metrics.output_wait_ns.fetch_add(
                                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - queue_begin).count(),
                                std::memory_order_relaxed);
                            if (!queued) throw std::runtime_error("object writer queue closed during backup");
                            is_owner = false; // The asynchronous completion now owns the claim and coordinator promise.
                            continue;
                        }
                        const bool queued = object_queue.push(std::move(blob), &errors.cancelled);
                        metrics.output_wait_ns.fetch_add(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - queue_begin).count(), std::memory_order_relaxed);
                        if (!queued) throw std::runtime_error("object writer queue closed during backup");
                        const auto ack_begin = Clock::now();
                        const WriteAck ack = ack_future.get();
                        const auto ack_wait = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - ack_begin).count();
                        metrics.writer_ack_wait_ns.fetch_add(ack_wait, std::memory_order_relaxed);
                        metrics.output_wait_ns.fetch_add(ack_wait, std::memory_order_relaxed);
                        claims.complete(claim_id, state, ack.header);
                        metrics.completed_chunks.fetch_add(1, std::memory_order_relaxed);
                        metrics.completed_raw_bytes.fetch_add(ack.header.raw_bytes, std::memory_order_relaxed);
                        task.promise->set_value(ack.header);
                    } catch (...) {
                        const auto failure = std::current_exception();
                        if (permit) gate.release();
                        if (is_owner && owner_state) claims.fail(claim_id, owner_state, failure);
                        try { task.promise->set_exception(failure); } catch (...) {}
                        fail(failure);
                    }
                }
            });

        // A fixed 2-second control window; sampling reads only counters and bounded queue state.
        monitor = std::thread([&] {
            auto last = Clock::now();
            std::uint64_t window_id = 0;
            std::uint64_t last_cpu = 0, last_output = 0, last_writer = 0, last_claim = 0;
            std::uint64_t last_controlled_cpu = 0;
            std::uint64_t last_hash = 0, last_compression = 0, last_decompression = 0;
            std::uint64_t last_object_verify = 0, last_object_io = 0;
            std::uint64_t last_source_scan = 0, last_source_read = 0, last_catalog_commit = 0;
            std::uint64_t last_object_setup = 0, last_object_staging_write = 0, last_object_file_sync = 0;
            std::uint64_t last_object_publish = 0, last_object_directory_sync = 0, last_object_cleanup = 0;
            std::uint64_t last_object_existing_check = 0, last_object_write_total = 0;
            std::uint64_t last_object_write_count = 0, last_object_write_operations = 0;
            std::uint64_t last_ack_wait = 0;
            std::uint64_t last_chunks = 0, last_bytes = 0, last_memory_wait = 0, last_gate_wait = 0;
            std::uint64_t last_unique_bytes = 0, last_stored_bytes = 0, last_progress_files = 0, last_errors = 0;
            AdaptiveController controller;
            double input_sum = 0, object_sum = 0, memory_sum = 0;
            std::uint64_t samples = 0;
            auto evaluate = [&](std::uint64_t elapsed_ns) {
                const auto controller_begin = Clock::now();
                ++window_id;
                const std::uint64_t cpu = metrics.cpu_busy_ns.load(std::memory_order_relaxed);
                const std::uint64_t controlled_cpu = metrics.controlled_cpu_ns.load(std::memory_order_relaxed);
                const std::uint64_t hash = metrics.hash_ns.load(std::memory_order_relaxed);
                const std::uint64_t compression = metrics.compression_ns.load(std::memory_order_relaxed);
                const std::uint64_t decompression = metrics.decompression_ns.load(std::memory_order_relaxed);
                const std::uint64_t object_verify = metrics.object_verify_ns.load(std::memory_order_relaxed);
                const std::uint64_t object_io = metrics.object_io_ns.load(std::memory_order_relaxed);
                const std::uint64_t source_scan = metrics.source_scan_ns.load(std::memory_order_relaxed);
                const std::uint64_t source_read = metrics.source_read_ns.load(std::memory_order_relaxed);
                const std::uint64_t catalog_commit = metrics.catalog_commit_ns.load(std::memory_order_relaxed);
                const std::uint64_t object_setup = metrics.object_setup_ns.load(std::memory_order_relaxed);
                const std::uint64_t object_staging_write = metrics.object_staging_write_ns.load(std::memory_order_relaxed);
                const std::uint64_t object_file_sync = metrics.object_file_sync_ns.load(std::memory_order_relaxed);
                const std::uint64_t object_publish = metrics.object_publish_ns.load(std::memory_order_relaxed);
                const std::uint64_t object_directory_sync = metrics.object_directory_sync_ns.load(std::memory_order_relaxed);
                const std::uint64_t object_cleanup = metrics.object_cleanup_ns.load(std::memory_order_relaxed);
                const std::uint64_t object_existing_check = metrics.object_existing_check_ns.load(std::memory_order_relaxed);
                const std::uint64_t object_write_total = metrics.object_write_total_ns.load(std::memory_order_relaxed);
                const std::uint64_t object_write_count = metrics.object_write_count.load(std::memory_order_relaxed);
                const std::uint64_t object_write_operations = metrics.object_write_operation_count.load(std::memory_order_relaxed);
                const std::uint64_t output = metrics.output_wait_ns.load(std::memory_order_relaxed);
                const std::uint64_t ack_wait = metrics.writer_ack_wait_ns.load(std::memory_order_relaxed);
                const std::uint64_t writer = metrics.writer_busy_ns.load(std::memory_order_relaxed);
                const std::uint64_t claim = metrics.claim_wait_ns.load(std::memory_order_relaxed);
                const std::uint64_t chunks = metrics.completed_chunks.load(std::memory_order_relaxed);
                const std::uint64_t bytes = metrics.completed_raw_bytes.load(std::memory_order_relaxed);
                const std::uint64_t unique_bytes = metrics.completed_unique_raw_bytes.load(std::memory_order_relaxed);
                const std::uint64_t stored_bytes = metrics.new_stored_bytes.load(std::memory_order_relaxed);
                const std::uint64_t progress_files = metrics.progress_file_count.load(std::memory_order_relaxed);
                const std::uint64_t errors_seen = errors.error_count.load(std::memory_order_relaxed);
                const std::uint64_t mem_wait = memory.wait_ns();
                const std::uint64_t gate_wait = gate.wait_ns();
                const std::size_t before = gate.limit();
                WindowSample m;
                m.elapsed_ns = elapsed_ns;
                m.controlled_cpu_ns = controlled_cpu - last_controlled_cpu;
                m.hash_ns = hash - last_hash;
                m.compression_ns = compression - last_compression;
                m.decompression_ns = decompression - last_decompression;
                m.object_verify_ns = object_verify - last_object_verify;
                m.object_io_ns = object_io - last_object_io;
                m.source_scan_ns = source_scan - last_source_scan;
                m.source_read_ns = source_read - last_source_read;
                m.catalog_commit_ns = catalog_commit - last_catalog_commit;
                m.object_setup_ns = object_setup - last_object_setup;
                m.object_staging_write_ns = object_staging_write - last_object_staging_write;
                m.object_file_sync_ns = object_file_sync - last_object_file_sync;
                m.object_publish_ns = object_publish - last_object_publish;
                m.object_directory_sync_ns = object_directory_sync - last_object_directory_sync;
                m.object_cleanup_ns = object_cleanup - last_object_cleanup;
                m.object_existing_check_ns = object_existing_check - last_object_existing_check;
                m.object_write_total_ns = object_write_total - last_object_write_total;
                m.object_write_count = object_write_count - last_object_write_count;
                m.object_write_operation_count = object_write_operations - last_object_write_operations;
                m.completed_chunks = chunks - last_chunks;
                m.completed_bytes = bytes - last_bytes;
                m.unique_raw_bytes = unique_bytes - last_unique_bytes;
                m.committed_object_bytes = stored_bytes - last_stored_bytes;
                m.progress_file_count = progress_files - last_progress_files;
                m.error_count = errors_seen - last_errors;
                m.writer_workers = options.writer_workers;
                m.input_queue_fill = samples ? input_sum / samples : 0;
                m.object_queue_fill = samples ? object_sum / samples : 0;
                m.memory_used_ratio = samples ? memory_sum / samples : 0;
                m.writer_busy_ratio = elapsed_ns ? std::clamp(
                    double(writer - last_writer) / (double(elapsed_ns) * options.writer_workers), 0.0, 1.0) : 0;
                m.cpu_compute_ratio = elapsed_ns ? std::clamp(double(cpu - last_cpu) / (elapsed_ns * before), 0.0, 1.0) : 0;
                m.cpu_admission_wait_ratio = elapsed_ns ? std::clamp(double(gate_wait - last_gate_wait) / (elapsed_ns * workers_count), 0.0, 1.0) : 0;
                m.output_wait_ratio = elapsed_ns ? std::clamp(double(output - last_output) / (elapsed_ns * before), 0.0, 1.0) : 0;
                m.writer_ack_wait_ratio = elapsed_ns ? std::clamp(double(ack_wait - last_ack_wait) / (elapsed_ns * before), 0.0, 1.0) : 0;
                m.claim_wait_ratio = elapsed_ns ? std::clamp(double(claim - last_claim) / (elapsed_ns * workers_count), 0.0, 1.0) : 0;
                m.memory_wait_ratio = elapsed_ns ? std::clamp(double(mem_wait - last_memory_wait) / elapsed_ns, 0.0, 1.0) : 0;
                m.throughput_bytes_per_sec = elapsed_ns ? double(m.completed_bytes) * 1e9 / elapsed_ns : 0;
                auto decision = controller.evaluate(m, classify(m), before, workers_count,
                    options.adaptive, errors.cancelled.load(std::memory_order_relaxed));
                if (decision.limit_changed) {
                    gate.set_limit(decision.active_limit);
                    metrics.worker_churn.fetch_add(1, std::memory_order_relaxed);
                }
                std::size_t after = decision.active_limit;
                Diagnosis d = std::move(decision.diagnosis);
                const std::string& action = decision.action;
                const std::string& outcome = decision.outcome;
                if (trace) {
                    trace << make_trace_line(run_id, window_id, summary.scheduler, before, after, m, d, action, outcome) << '\n';
                    trace.flush();
                }
                metrics.controller_overhead_ns.fetch_add(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - controller_begin).count(),
                    std::memory_order_relaxed);
                last_cpu = cpu; last_output = output; last_writer = writer; last_claim = claim; last_ack_wait = ack_wait;
                last_controlled_cpu = controlled_cpu;
                last_hash = hash; last_compression = compression; last_decompression = decompression;
                last_object_verify = object_verify; last_object_io = object_io;
                last_source_scan = source_scan; last_source_read = source_read; last_catalog_commit = catalog_commit;
                last_object_setup = object_setup; last_object_staging_write = object_staging_write;
                last_object_file_sync = object_file_sync; last_object_publish = object_publish;
                last_object_directory_sync = object_directory_sync; last_object_cleanup = object_cleanup;
                last_object_existing_check = object_existing_check; last_object_write_total = object_write_total;
                last_object_write_count = object_write_count; last_object_write_operations = object_write_operations;
                last_chunks = chunks; last_bytes = bytes; last_memory_wait = mem_wait; last_gate_wait = gate_wait;
                last_unique_bytes = unique_bytes; last_stored_bytes = stored_bytes; last_progress_files = progress_files; last_errors = errors_seen;
                input_sum = object_sum = memory_sum = 0; samples = 0;
            };
            while (!monitor_stop.load(std::memory_order_relaxed) && !errors.cancelled.load(std::memory_order_relaxed)) {
                if (interrupt_requested()) {
                    fail(std::make_exception_ptr(std::runtime_error("backup cancelled by interrupt")));
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                input_sum += double(input_queue.size()) / input_queue.capacity();
                object_sum += double(object_queue.size()) / object_queue.capacity();
                memory_sum += double(memory.current()) / memory.limit();
                const std::size_t rss = current_rss_bytes();
                std::size_t old_rss = metrics.process_rss_peak_bytes.load(std::memory_order_relaxed);
                while (rss > old_rss && !metrics.process_rss_peak_bytes.compare_exchange_weak(old_rss, rss, std::memory_order_relaxed)) {}
                ++samples;
                const auto now = Clock::now();
                const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(now - last).count();
                if (elapsed >= 2000000000ll) { evaluate(static_cast<std::uint64_t>(elapsed)); last = now; }
            }
            const auto now = Clock::now();
            const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(now - last).count();
            if (elapsed > 0) evaluate(static_cast<std::uint64_t>(elapsed));
        });
        monitor_started = true;

        std::string previous_snapshot;
        std::unordered_map<std::string, CachedEntry> metadata_cache;
        if (options.trust_metadata && !options.paranoid)
            metadata_cache = load_metadata_cache(catalog.get(), source_text, previous_snapshot);
        const auto source_scan_begin = Clock::now();
        const auto entries = scan_source(source, options.skip_unsupported, errors.cancelled);
        metrics.source_scan_ns.fetch_add(elapsed_ns(source_scan_begin), std::memory_order_relaxed);
        for (const auto& entry : entries) {
            if (errors.cancelled.load(std::memory_order_relaxed)) errors.rethrow();
            if (entry.kind == "directory") {
                snapshot_entry_transaction(catalog.get(), snapshot_id, entry, options.failpoint);
                ++summary.directories;
                continue;
            }
            if (entry.kind == "symlink") {
                snapshot_entry_transaction(catalog.get(), snapshot_id, entry, options.failpoint);
                ++summary.symlinks;
                continue;
            }
            ++summary.files;
            summary.logical_bytes += entry.size;
            auto cache = metadata_cache.find(entry.relative);
            if (cache != metadata_cache.end() && cache->second.kind == "file" &&
                cache->second.size == entry.size && cache->second.mode == entry.mode && cache->second.mtime_ns == entry.mtime_ns) {
                snapshot_entry_transaction(catalog.get(), snapshot_id, entry, options.failpoint);
                constexpr std::size_t cache_batch_size = 128;
                std::uint64_t next_ordinal = 0;
                std::uint64_t expected_ordinal = 0;
                std::uint64_t cached_bytes = 0;
                for (;;) {
                    std::vector<ChunkRef> refs;
                    {
                        const char* query_sql = packed_storage
                            ? "SELECT c.ordinal,c.object_id,c.chunk_bytes,o.pack_id,o.pack_offset FROM entry_chunks c JOIN objects o ON o.object_id=c.object_id WHERE c.snapshot_id=? AND c.relative_path=? AND c.ordinal>=? ORDER BY c.ordinal LIMIT ?"
                            : "SELECT ordinal,object_id,chunk_bytes FROM entry_chunks WHERE snapshot_id=? AND relative_path=? AND ordinal>=? ORDER BY ordinal LIMIT ?";
                        Statement query(catalog.get(), query_sql);
                        query.text(1, previous_snapshot); query.text(2, entry.relative);
                        query.integer(3, static_cast<std::int64_t>(next_ordinal)); query.integer(4, cache_batch_size);
                        while (query.row()) {
                            ChunkRef ref{static_cast<std::uint64_t>(query.i64(0)),
                                static_cast<std::uint64_t>(query.i64(2)), query.string(1), {}};
                            if (packed_storage) {
                                if (query.is_null(3) || query.is_null(4))
                                    throw std::runtime_error("cached packed object is missing its catalog location");
                                ref.location = ObjectLocation{query.string(3), static_cast<std::uint64_t>(query.i64(4))};
                            }
                            refs.push_back(std::move(ref));
                        }
                    }
                    if (refs.empty()) break;
                    std::vector<ObjectHeader> headers;
                    headers.reserve(refs.size());
                    const std::uint64_t batch_start_bytes = cached_bytes;
                    for (const auto& ref : refs) {
                        const std::uint64_t expected_bytes = entry.size > cached_bytes
                            ? std::min<std::uint64_t>(kChunkSize, entry.size - cached_bytes) : 0;
                        if (ref.ordinal != expected_ordinal || expected_bytes == 0 || ref.bytes != expected_bytes)
                            throw std::runtime_error("cached chunk map is invalid for " + entry.relative);
                        const auto header_begin = Clock::now();
                        auto header = packed_storage
                            ? detail::read_packed_object_header(repo, ref.location)
                            : read_object_header(object_path(repo, ref.object_id));
                        metrics.object_io_ns.fetch_add(elapsed_ns(header_begin), std::memory_order_relaxed);
                        if (header.raw_bytes != ref.bytes || digest_hex(header.digest) != ref.object_id)
                            throw std::runtime_error("cached object metadata mismatch");
                        const bool verify_object = !verified_existing_objects.contains_or_insert(ref.object_id);
                        if (verify_object) {
                            if (!gate.acquire(errors.cancelled)) errors.rethrow();
                            try {
                                auto credit = memory.acquire(credit_per_chunk, errors.cancelled);
                                ObjectReadTimings timings;
                                const auto raw = packed_storage
                                    ? detail::read_packed_object(repo, ref.location, ref.object_id, &timings)
                                    : read_object(object_path(repo, ref.object_id), ref.object_id, &timings);
                                record_object_read(metrics, timings);
                                if (raw.size() != ref.bytes)
                                    throw std::runtime_error("cached object content length mismatch");
                            } catch (...) {
                                gate.release();
                                throw;
                            }
                            gate.release();
                        }
                        headers.push_back(header);
                        ++expected_ordinal;
                        cached_bytes += ref.bytes;
                    }
                    const auto catalog_begin = Clock::now();
                    insert_chunk_batch(catalog.get(), snapshot_id, entry.relative, refs, headers, options.failpoint);
                    metrics.catalog_commit_ns.fetch_add(elapsed_ns(catalog_begin), std::memory_order_relaxed);
                    summary.reused_chunks += refs.size();
                    metrics.completed_chunks.fetch_add(refs.size(), std::memory_order_relaxed);
                    metrics.completed_raw_bytes.fetch_add(cached_bytes - batch_start_bytes, std::memory_order_relaxed);
                    next_ordinal = refs.back().ordinal + 1;
                }
                if (cached_bytes != entry.size)
                    throw std::runtime_error("cached chunk map is incomplete for " + entry.relative);
                metrics.progress_file_count.fetch_add(1, std::memory_order_relaxed);
                continue;
            }

            int fd = ::open(entry.absolute.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
            if (fd < 0) throw std::runtime_error(error_text("open source file " + entry.absolute.string()));
            struct stat before{};
            if (::fstat(fd, &before) != 0) { const auto e = error_text("stat open source file"); ::close(fd); throw std::runtime_error(e); }
            if (!S_ISREG(before.st_mode) || static_cast<std::uint64_t>(before.st_size) != entry.size || stat_mtime_ns(before) != entry.mtime_ns ||
                stat_ctime_ns(before) != entry.ctime_ns || static_cast<std::uint64_t>(before.st_dev) != entry.device ||
                static_cast<std::uint64_t>(before.st_ino) != entry.inode ||
                static_cast<std::uint32_t>(before.st_mode & 07777) != entry.mode) {
                ::close(fd);
                throw std::runtime_error("source changed between scan and read: " + entry.relative);
            }

            try { snapshot_entry_transaction(catalog.get(), snapshot_id, entry, options.failpoint); }
            catch (...) { ::close(fd); throw; }
            std::vector<std::future<ObjectHeader>> futures;
            std::vector<std::uint64_t> chunk_sizes;
            std::uint64_t offset = 0, ordinal = 0;
            const std::size_t metadata_batch_size = coordinator_batch_size;
            auto flush_chunk_batch = [&] {
                if (futures.empty()) return;
                std::vector<ChunkRef> refs;
                std::vector<ObjectHeader> headers;
                refs.reserve(futures.size()); headers.reserve(futures.size());
                const std::size_t count = futures.size();
                for (std::size_t i = 0; i < count; ++i) {
                    const ObjectHeader header = futures[i].get();
                    const auto id = digest_hex(header.digest);
                    refs.push_back(ChunkRef{ordinal - count + i, chunk_sizes[i], id, {}});
                    headers.push_back(header);
                    if (header.newly_written) summary.unique_raw_bytes += header.raw_bytes;
                    else ++summary.reused_chunks;
                }
                const auto catalog_begin = Clock::now();
                insert_chunk_batch(catalog.get(), snapshot_id, entry.relative, refs, headers, options.failpoint);
                metrics.catalog_commit_ns.fetch_add(elapsed_ns(catalog_begin), std::memory_order_relaxed);
                for (const auto& ref : refs) claims.release(ref.object_id);
                futures.clear();
                chunk_sizes.clear();
            };
            while (offset < entry.size) {
                const std::size_t want = static_cast<std::size_t>(std::min<std::uint64_t>(kChunkSize, entry.size - offset));
                auto credit = memory.acquire(credit_per_chunk, errors.cancelled);
                std::vector<std::uint8_t> raw(want);
                std::size_t read_count = 0;
                const auto source_read_begin = Clock::now();
                while (read_count < want) {
                    const ssize_t n = ::read(fd, raw.data() + read_count, want - read_count);
                    if (n < 0 && errno == EINTR) continue;
                    if (n <= 0) { ::close(fd); throw std::runtime_error("source file became shorter while reading: " + entry.relative); }
                    read_count += static_cast<std::size_t>(n);
                }
                metrics.source_read_ns.fetch_add(elapsed_ns(source_read_begin), std::memory_order_relaxed);
                auto promise = std::make_shared<std::promise<ObjectHeader>>();
                futures.push_back(promise->get_future());
                chunk_sizes.push_back(want);
                ChunkTask task;
                task.ordinal = ordinal++;
                task.raw = std::move(raw);
                task.credit = std::move(credit);
                task.promise = std::move(promise);
                if (!input_queue.push(std::move(task), &errors.cancelled)) {
                    ::close(fd);
                    errors.rethrow();
                    throw std::runtime_error("chunk queue closed during backup");
                }
                offset += want;
                summary.read_bytes += want;
                if (!sleep_cancellable(options.test_reader_delay_ms, errors.cancelled)) {
                    ::close(fd);
                    throw std::runtime_error("backup cancelled during source read");
                }
                if (futures.size() >= metadata_batch_size) flush_chunk_batch();
            }
            struct stat after{};
            if (::fstat(fd, &after) != 0) { const auto e = error_text("stat source file after read"); ::close(fd); throw std::runtime_error(e); }
            ::close(fd);
            if (before.st_dev != after.st_dev || before.st_ino != after.st_ino || before.st_size != after.st_size ||
                stat_mtime_ns(before) != stat_mtime_ns(after) || stat_ctime_ns(before) != stat_ctime_ns(after))
                throw std::runtime_error("source changed while being read: " + entry.relative);
            flush_chunk_batch();
            metrics.progress_file_count.fetch_add(1, std::memory_order_relaxed);
        }

        finish_threads(false);
        errors.rethrow();
        if (interrupt_requested()) throw std::runtime_error("backup cancelled by interrupt");
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
        summary.new_stored_bytes = metrics.new_stored_bytes.load(std::memory_order_relaxed);
        summary.memory_credit_peak_bytes = memory.peak();
        summary.process_rss_peak_bytes = metrics.process_rss_peak_bytes.load(std::memory_order_relaxed);
        summary.worker_churn = metrics.worker_churn.load(std::memory_order_relaxed);
        summary.controller_overhead_seconds = metrics.controller_overhead_ns.load(std::memory_order_relaxed) / 1e9;
        summary.elapsed_seconds = std::chrono::duration<double>(Clock::now() - start).count();
        static_cast<SnapshotSummary&>(summary) = query_snapshot(catalog.get(), snapshot_id);
        {
            std::ofstream report(repo / "reports" / (run_id + ".summary.json"));
            if (report) report << "{\"schema_version\":1,\"run_id\":\"" << json_escape(run_id)
                << "\",\"snapshot_id\":\"" << json_escape(snapshot_id) << "\",\"scheduler\":\"" << summary.scheduler
                << "\",\"workers\":" << workers_count << ",\"logical_bytes\":" << summary.logical_bytes
                << ",\"writer_workers\":" << summary.writer_workers
                << ",\"read_bytes\":" << summary.read_bytes << ",\"unique_raw_bytes\":" << summary.unique_raw_bytes
                << ",\"new_stored_bytes\":" << summary.new_stored_bytes << ",\"reused_chunks\":" << summary.reused_chunks
                << ",\"memory_credit_peak_bytes\":" << memory.peak() << ",\"memory_budget_bytes\":" << memory.limit()
                << ",\"process_rss_peak_bytes\":" << summary.process_rss_peak_bytes
                << ",\"worker_churn\":" << summary.worker_churn << ",\"controller_overhead_seconds\":" << summary.controller_overhead_seconds
                << ",\"elapsed_seconds\":" << summary.elapsed_seconds << "}\n";
        }
        return summary;
    } catch (...) {
        const auto failure = std::current_exception();
        fail(failure);
        finish_threads(true);
        try {
            Transaction transaction(catalog.get());
            set_snapshot_status(catalog.get(), snapshot_id, "FAILED", false);
            transaction.commit();
        } catch (...) {}
        std::error_code ec;
        std::filesystem::remove_all(staging_dir, ec);
        std::rethrow_exception(failure);
    }
}

std::vector<SnapshotSummary> list_snapshots(const std::filesystem::path& repo_input) {
    validate_repo(repo_input);
    const auto repo = canonical_existing(repo_input);
    Catalog catalog(repo / "catalog.sqlite");
    Statement statement(catalog.get(), "SELECT id FROM snapshots WHERE status='COMPLETE' ORDER BY created_ns DESC,id DESC");
    std::vector<SnapshotSummary> snapshots;
    while (statement.row()) snapshots.push_back(query_snapshot(catalog.get(), statement.string(0)));
    return snapshots;
}

SnapshotSummary show_snapshot(const std::filesystem::path& repo_input, const std::string& snapshot) {
    validate_repo(repo_input);
    const auto repo = canonical_existing(repo_input);
    Catalog catalog(repo / "catalog.sqlite");
    require_complete(catalog.get(), snapshot);
    return query_snapshot(catalog.get(), snapshot);
}

VerifySummary verify_snapshot(const std::filesystem::path& repo_input, const std::string& snapshot) {
    validate_repo(repo_input);
    const auto repo = canonical_existing(repo_input);
    const bool packed_storage = detail::repository_format_version(repo) == 2;
    Catalog catalog(repo / "catalog.sqlite");
    require_complete(catalog.get(), snapshot);
    exec(catalog.get(), "PRAGMA foreign_keys=ON;");
    Statement foreign_keys(catalog.get(), "PRAGMA foreign_key_check");
    if (foreign_keys.row()) throw std::runtime_error("catalog foreign key integrity check failed");
    VerifySummary result;
    std::unordered_map<std::string, std::uint64_t> verified_objects;
    for (const auto& entry : load_entries(catalog.get(), snapshot)) {
        (void)path_parts(entry.path);
        if (entry.kind == "directory") continue;
        if (entry.kind == "symlink") {
            if (entry.symlink_target.find('\0') != std::string::npos) throw std::runtime_error("invalid symlink target in catalog");
            ++result.files;
            continue;
        }
        if (entry.kind != "file") throw std::runtime_error("unknown entry kind in catalog");
        ++result.files;
        const auto refs = load_chunks(catalog.get(), snapshot, entry.path, packed_storage);
        std::uint64_t total = 0;
        for (std::size_t i = 0; i < refs.size(); ++i) {
            const auto& ref = refs[i];
            if (ref.ordinal != i) throw std::runtime_error("non-contiguous chunk ordinals in " + entry.path);
            const std::uint64_t expected = std::min<std::uint64_t>(kChunkSize, entry.size > total ? entry.size - total : 0);
            if (ref.bytes != expected || ref.bytes == 0) throw std::runtime_error("invalid chunk length in " + entry.path);
            auto verified = verified_objects.find(ref.object_id);
            if (verified != verified_objects.end()) {
                if (verified->second != ref.bytes) throw std::runtime_error("one object is referenced with inconsistent raw lengths: " + ref.object_id);
            } else {
                const ObjectHeader header = packed_storage
                    ? detail::read_packed_object_header(repo, ref.location)
                    : read_object_header(object_path(repo, ref.object_id));
                if (header.raw_bytes != ref.bytes) throw std::runtime_error("object and reference length differ: " + ref.object_id);
                Statement object_row(catalog.get(), packed_storage
                    ? "SELECT raw_bytes,stored_bytes,codec,pack_id,pack_offset FROM objects WHERE object_id=?"
                    : "SELECT raw_bytes,stored_bytes,codec FROM objects WHERE object_id=?");
                object_row.text(1, ref.object_id);
                if (!object_row.row() || static_cast<std::uint64_t>(object_row.i64(0)) != header.raw_bytes ||
                    static_cast<std::uint64_t>(object_row.i64(1)) != header.stored_bytes ||
                    object_row.string(2) != (header.codec ? "zstd" : "raw") ||
                    (packed_storage && (object_row.is_null(3) || object_row.is_null(4) ||
                        object_row.string(3) != ref.location.pack_id ||
                        static_cast<std::uint64_t>(object_row.i64(4)) != ref.location.pack_offset)))
                    throw std::runtime_error("catalog object metadata differs from object header: " + ref.object_id);
                const auto raw = packed_storage
                    ? detail::read_packed_object(repo, ref.location, ref.object_id)
                    : read_object(object_path(repo, ref.object_id), ref.object_id);
                if (raw.size() != ref.bytes) throw std::runtime_error("object and reference length differ: " + ref.object_id);
                verified_objects.emplace(ref.object_id, ref.bytes);
            }
            total += ref.bytes;
            ++result.chunks;
        }
        if (total != entry.size) throw std::runtime_error("file size and chunk map differ: " + entry.path);
        result.bytes += total;
    }
    ensure_chunk_layout(catalog.get(), snapshot);
    return result;
}

RepositoryStats repository_stats(const std::filesystem::path& repo_input) {
    validate_repo(repo_input);
    const auto repo = canonical_existing(repo_input);
    const bool packed_storage = detail::repository_format_version(repo) == 2;
    Catalog catalog(repo / "catalog.sqlite");
    RepositoryStats stats;
    {
        Statement statement(catalog.get(), "SELECT COUNT(*) FROM snapshots WHERE status='COMPLETE'");
        if (statement.row()) stats.snapshots = static_cast<std::uint64_t>(statement.i64(0));
    }
    {
        Statement statement(catalog.get(), "SELECT COALESCE(SUM(e.size_bytes),0) FROM entries e JOIN snapshots s ON s.id=e.snapshot_id WHERE s.status='COMPLETE' AND e.kind='file'");
        if (statement.row()) stats.logical_bytes = static_cast<std::uint64_t>(statement.i64(0));
    }
    {
        Statement statement(catalog.get(), "SELECT COUNT(DISTINCT c.object_id) FROM entry_chunks c JOIN snapshots s ON s.id=c.snapshot_id WHERE s.status='COMPLETE'");
        if (statement.row()) stats.referenced_objects = static_cast<std::uint64_t>(statement.i64(0));
    }
    if (packed_storage) {
        Statement statement(catalog.get(), "SELECT COUNT(*) FROM objects");
        if (statement.row()) stats.object_count = static_cast<std::uint64_t>(statement.i64(0));
    }
    std::error_code ec;
    const auto root = repo / "objects";
    if (std::filesystem::exists(root, ec)) {
        for (std::filesystem::recursive_directory_iterator it(root, std::filesystem::directory_options::skip_permission_denied, ec), end; it != end; it.increment(ec)) {
            if (ec) { ec.clear(); continue; }
            if (!it->is_regular_file(ec) || it->path().extension() != (packed_storage ? ".pack" : ".obj")) continue;
            if (!packed_storage) ++stats.object_count;
            const auto size = it->file_size(ec);
            if (!ec) stats.physical_bytes += size;
            else ec.clear();
        }
    }
    return stats;
}

std::string explain_run(const std::filesystem::path& repo_input, const std::string& run_id, bool json) {
    validate_repo(repo_input);
    if (run_id.empty() || !std::all_of(run_id.begin(), run_id.end(), [](unsigned char c) { return std::isalnum(c) || c == '-' || c == '_'; }))
        throw std::runtime_error("invalid run id");
    const auto repo = canonical_existing(repo_input);
    const auto path = repo / "reports" / (run_id + ".jsonl");
    std::ifstream in(path);
    if (!in) throw std::runtime_error("decision trace not found for run " + run_id);
    std::map<std::string, std::size_t> diagnoses, actions;
    std::vector<std::string> reasons;
    std::string line;
    std::size_t windows = 0;
    auto get_string = [](const std::string& text, const std::string& key) {
        const std::string needle = "\"" + key + "\":\"";
        const auto start = text.find(needle);
        if (start == std::string::npos) return std::string("unknown");
        const auto value_start = start + needle.size();
        const auto end = text.find('"', value_start);
        return end == std::string::npos ? std::string("unknown") : text.substr(value_start, end - value_start);
    };
    while (std::getline(in, line)) {
        ++windows;
        ++diagnoses[get_string(line, "diagnosis")];
        ++actions[get_string(line, "action")];
        const auto reason = get_string(line, "reason");
        if (reason != "unknown" && !reasons.empty() && reasons.back() != reason) reasons.push_back(reason);
        else if (reason != "unknown" && reasons.empty()) reasons.push_back(reason);
    }
    if (!windows) throw std::runtime_error("decision trace is empty");
    std::ostringstream out;
    if (json) {
        out << "{\"run_id\":\"" << json_escape(run_id) << "\",\"windows\":" << windows << ",\"diagnoses\":{";
        bool first = true;
        for (const auto& [name, count] : diagnoses) {
            if (!first) out << ',';
            first = false;
            out << '"' << json_escape(name) << "\":" << count;
        }
        out << "},\"actions\":{";
        first = true;
        for (const auto& [name, count] : actions) {
            if (!first) out << ',';
            first = false;
            out << '"' << json_escape(name) << "\":" << count;
        }
        out << "},\"evidence\":[";
        for (std::size_t i = 0; i < reasons.size(); ++i) {
            if (i) out << ',';
            out << '"' << json_escape(reasons[i]) << '"';
        }
        out << "],\"interpretation\":\"These are pipeline-stage observations and hypotheses; they do not prove a hardware-level cause.\""
            << ",\"suggested_followups\":["
            << "\"Run the same dataset in a fresh repository with fixed workers 1, 2, 4, and 8.\","
            << "\"Compare a compressible text dataset with incompressible seeded random data.\","
            << "\"Repeat with a smaller memory budget and compare credit waits, queue fill, and completion time.\"]}\n";
        return out.str();
    }
    out << "Run: " << run_id << "\nWindows: " << windows << "\n\nObserved diagnoses:\n";
    for (const auto& [name, count] : diagnoses) out << "  " << name << ": " << count << " window(s)\n";
    out << "\nController actions:\n";
    for (const auto& [name, count] : actions) out << "  " << name << ": " << count << " window(s)\n";
    out << "\nEvidence and interpretation:\n";
    for (const auto& reason : reasons) out << "  - " << reason << "\n";
    out << "These are pipeline-stage observations and hypotheses; they do not prove a hardware-level cause.\n\n"
        << "Suggested reproducible follow-ups:\n"
        << "  1. Run the same dataset in a fresh repository with fixed workers 1, 2, 4, and 8.\n"
        << "  2. Compare a compressible text dataset with incompressible seeded random data.\n"
        << "  3. Repeat with a smaller memory budget and compare credit waits, queue fill, and completion time.\n";
    return out.str();
}



} // namespace pbe

