#include "pbe/engine.hpp"
#include "storage/catalog.hpp"
#include "storage/object_store.hpp"
#include "storage/repository.hpp"
#include "storage/snapshot_catalog.hpp"
#include "pbe/blake3.hpp"

#include <algorithm>
#include <cerrno>
#include <cctype>
#include <csignal>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <unistd.h>
#include <unordered_map>

namespace pbe {
namespace {
volatile std::sig_atomic_t g_interrupt_requested = 0;
}

void note_interrupt() noexcept {
    g_interrupt_requested = 1;
}

bool interrupt_requested() noexcept {
    return g_interrupt_requested != 0;
}

namespace {
using detail::Catalog;
using detail::ChunkRef;
using detail::ObjectHeader;
using detail::Statement;
using detail::create_schema;
using detail::exec;
using detail::fsync_directory;
using detail::kChunkSize;
using detail::object_path;
using detail::read_object;
using detail::read_object_header;
using detail::canonical_existing;
using detail::ensure_chunk_layout;
using detail::load_chunks;
using detail::load_entries;
using detail::path_parts;
using detail::query_snapshot;
using detail::require_complete;
using detail::validate_repo;

std::string error_text(std::string_view operation) {
    return std::string(operation) + ": " + std::strerror(errno);
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

} // namespace

void init_repository(const std::filesystem::path& input, const std::string& storage_format) {
    if (input.empty())
        throw std::runtime_error("--repo is required");
    if (storage_format != "files-v1" && storage_format != "pack-v2")
        throw std::runtime_error("storage format must be files-v1 or pack-v2");
    std::error_code ec;
    auto repo = std::filesystem::absolute(input, ec).lexically_normal();
    if (ec)
        throw std::runtime_error("cannot resolve repository path: " + ec.message());
    bool created = false;
    if (!std::filesystem::exists(repo, ec)) {
        if (!std::filesystem::create_directories(repo, ec) || ec)
            throw std::runtime_error("cannot create repository directory: " + ec.message());
        created = true;
    }
    if (!std::filesystem::is_directory(repo))
        throw std::runtime_error("repository path is not a directory");
    if (!created &&
        std::filesystem::directory_iterator(repo) != std::filesystem::directory_iterator{})
        throw std::runtime_error("refusing to initialize a non-empty directory");
    std::filesystem::create_directories(repo / "objects");
    if (storage_format == "pack-v2")
        std::filesystem::create_directories(repo / "objects" / "packs");
    std::filesystem::create_directories(repo / "staging");
    std::filesystem::create_directories(repo / "reports");
    {
        std::ofstream config(repo / "repo.toml",
                             std::ios::binary | std::ios::out | std::ios::trunc);
        if (!config)
            throw std::runtime_error("cannot create repository config");
        config
            << "format_version = " << (storage_format == "pack-v2" ? 2 : 1)
            << "\nchunk_size = 1048576\nhash_algorithm = \"blake3-256\"\nobject_format = 1\nstorage_format = \""
            << storage_format << "\"\n";
        config.flush();
        if (!config)
            throw std::runtime_error("cannot write repository config");
    }
    int config_fd = ::open((repo / "repo.toml").c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (config_fd < 0)
        throw std::runtime_error(error_text("open repository config for sync"));
    if (::fsync(config_fd) != 0) {
        const auto e = error_text("sync repository config");
        ::close(config_fd);
        throw std::runtime_error(e);
    }
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
    if (lockfd >= 0) {
        ::fsync(lockfd);
        ::close(lockfd);
    }
    fsync_directory(repo);
}

std::vector<SnapshotSummary> list_snapshots(const std::filesystem::path& repo_input) {
    validate_repo(repo_input);
    const auto repo = canonical_existing(repo_input);
    Catalog catalog(repo / "catalog.sqlite");
    Statement statement(
        catalog.get(),
        "SELECT id FROM snapshots WHERE status='COMPLETE' ORDER BY created_ns DESC,id DESC");
    std::vector<SnapshotSummary> snapshots;
    while (statement.row())
        snapshots.push_back(query_snapshot(catalog.get(), statement.string(0)));
    return snapshots;
}

SnapshotSummary show_snapshot(const std::filesystem::path& repo_input,
                              const std::string& snapshot) {
    validate_repo(repo_input);
    const auto repo = canonical_existing(repo_input);
    Catalog catalog(repo / "catalog.sqlite");
    require_complete(catalog.get(), snapshot);
    return query_snapshot(catalog.get(), snapshot);
}

VerifySummary verify_snapshot(const std::filesystem::path& repo_input,
                              const std::string& snapshot) {
    // Full verification runs outside the backup pipeline and checks references, layout, and digests.
    validate_repo(repo_input);
    const auto repo = canonical_existing(repo_input);
    const bool packed_storage = detail::repository_format_version(repo) == 2;
    Catalog catalog(repo / "catalog.sqlite");
    require_complete(catalog.get(), snapshot);
    exec(catalog.get(), "PRAGMA foreign_keys=ON;");
    Statement foreign_keys(catalog.get(), "PRAGMA foreign_key_check");
    if (foreign_keys.row())
        throw std::runtime_error("catalog foreign key integrity check failed");
    VerifySummary result;

    // Verify shared content once per run; later references still validate the expected raw length.
    std::unordered_map<std::string, std::uint64_t> verified_objects;
    for (const auto& entry : load_entries(catalog.get(), snapshot)) {
        (void)path_parts(entry.path);
        if (entry.kind == "directory")
            continue;
        if (entry.kind == "symlink") {
            if (entry.symlink_target.find('\0') != std::string::npos)
                throw std::runtime_error("invalid symlink target in catalog");
            ++result.files;
            continue;
        }
        if (entry.kind != "file")
            throw std::runtime_error("unknown entry kind in catalog");
        ++result.files;
        const auto refs = load_chunks(catalog.get(), snapshot, entry.path, packed_storage);
        std::uint64_t total = 0;
        for (std::size_t i = 0; i < refs.size(); ++i) {
            const auto& ref = refs[i];
            if (ref.ordinal != i)
                throw std::runtime_error("non-contiguous chunk ordinals in " + entry.path);
            const std::uint64_t expected =
                std::min<std::uint64_t>(kChunkSize, entry.size > total ? entry.size - total : 0);
            if (ref.bytes != expected || ref.bytes == 0)
                throw std::runtime_error("invalid chunk length in " + entry.path);
            auto verified = verified_objects.find(ref.object_id);
            if (verified != verified_objects.end()) {
                if (verified->second != ref.bytes)
                    throw std::runtime_error(
                        "one object is referenced with inconsistent raw lengths: " + ref.object_id);
            } else {
                const ObjectHeader header =
                    packed_storage ? detail::read_packed_object_header(repo, ref.location)
                                   : read_object_header(object_path(repo, ref.object_id));
                if (header.raw_bytes != ref.bytes)
                    throw std::runtime_error("object and reference length differ: " +
                                             ref.object_id);
                Statement object_row(
                    catalog.get(),
                    packed_storage
                        ? "SELECT raw_bytes,stored_bytes,codec,pack_id,pack_offset FROM objects WHERE object_id=?"
                        : "SELECT raw_bytes,stored_bytes,codec FROM objects WHERE object_id=?");
                object_row.text(1, ref.object_id);
                if (!object_row.row() ||
                    static_cast<std::uint64_t>(object_row.i64(0)) != header.raw_bytes ||
                    static_cast<std::uint64_t>(object_row.i64(1)) != header.stored_bytes ||
                    object_row.string(2) != (header.codec ? "zstd" : "raw") ||
                    (packed_storage &&
                     (object_row.is_null(3) || object_row.is_null(4) ||
                      object_row.string(3) != ref.location.pack_id ||
                      static_cast<std::uint64_t>(object_row.i64(4)) != ref.location.pack_offset)))
                    throw std::runtime_error(
                        "catalog object metadata differs from object header: " + ref.object_id);
                const auto raw = packed_storage
                                     ? detail::read_packed_object(repo, ref.location, ref.object_id)
                                     : read_object(object_path(repo, ref.object_id), ref.object_id);
                if (raw.size() != ref.bytes)
                    throw std::runtime_error("object and reference length differ: " +
                                             ref.object_id);
                verified_objects.emplace(ref.object_id, ref.bytes);
            }
            total += ref.bytes;
            ++result.chunks;
        }
        if (total != entry.size)
            throw std::runtime_error("file size and chunk map differ: " + entry.path);
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
        Statement statement(catalog.get(),
                            "SELECT COUNT(*) FROM snapshots WHERE status='COMPLETE'");
        if (statement.row())
            stats.snapshots = static_cast<std::uint64_t>(statement.i64(0));
    }
    {
        Statement statement(
            catalog.get(),
            "SELECT COALESCE(SUM(e.size_bytes),0) FROM entries e JOIN snapshots s ON s.id=e.snapshot_id WHERE s.status='COMPLETE' AND e.kind='file'");
        if (statement.row())
            stats.logical_bytes = static_cast<std::uint64_t>(statement.i64(0));
    }
    {
        Statement statement(
            catalog.get(),
            "SELECT COUNT(DISTINCT c.object_id) FROM entry_chunks c JOIN snapshots s ON s.id=c.snapshot_id WHERE s.status='COMPLETE'");
        if (statement.row())
            stats.referenced_objects = static_cast<std::uint64_t>(statement.i64(0));
    }
    if (packed_storage) {
        Statement statement(catalog.get(), "SELECT COUNT(*) FROM objects");
        if (statement.row())
            stats.object_count = static_cast<std::uint64_t>(statement.i64(0));
    }
    std::error_code ec;
    const auto root = repo / "objects";
    if (std::filesystem::exists(root, ec)) {
        for (std::filesystem::recursive_directory_iterator
                 it(root, std::filesystem::directory_options::skip_permission_denied, ec),
             end;
             it != end; it.increment(ec)) {
            if (ec) {
                ec.clear();
                continue;
            }
            if (!it->is_regular_file(ec) ||
                it->path().extension() != (packed_storage ? ".pack" : ".obj"))
                continue;
            if (!packed_storage)
                ++stats.object_count;
            const auto size = it->file_size(ec);
            if (!ec)
                stats.physical_bytes += size;
            else
                ec.clear();
        }
    }
    return stats;
}

std::string explain_run(const std::filesystem::path& repo_input, const std::string& run_id,
                        bool json) {
    validate_repo(repo_input);
    if (run_id.empty() || !std::all_of(run_id.begin(), run_id.end(), [](unsigned char c) {
            return std::isalnum(c) || c == '-' || c == '_';
        }))
        throw std::runtime_error("invalid run id");
    const auto repo = canonical_existing(repo_input);
    const auto path = repo / "reports" / (run_id + ".jsonl");
    std::ifstream in(path);
    if (!in)
        throw std::runtime_error("decision trace not found for run " + run_id);
    std::map<std::string, std::size_t> diagnoses, actions;
    std::vector<std::string> reasons;
    std::string line;
    std::size_t windows = 0;
    auto get_string = [](const std::string& text, const std::string& key) {
        const std::string needle = "\"" + key + "\":\"";
        const auto start = text.find(needle);
        if (start == std::string::npos)
            return std::string("unknown");
        const auto value_start = start + needle.size();
        const auto end = text.find('"', value_start);
        return end == std::string::npos ? std::string("unknown")
                                        : text.substr(value_start, end - value_start);
    };
    while (std::getline(in, line)) {
        ++windows;
        ++diagnoses[get_string(line, "diagnosis")];
        ++actions[get_string(line, "action")];
        const auto reason = get_string(line, "reason");
        if (reason != "unknown" && !reasons.empty() && reasons.back() != reason)
            reasons.push_back(reason);
        else if (reason != "unknown" && reasons.empty())
            reasons.push_back(reason);
    }
    if (!windows)
        throw std::runtime_error("decision trace is empty");
    std::ostringstream out;
    if (json) {
        out << "{\"run_id\":\"" << json_escape(run_id) << "\",\"windows\":" << windows
            << ",\"diagnoses\":{";
        bool first = true;
        for (const auto& [name, count] : diagnoses) {
            if (!first)
                out << ',';
            first = false;
            out << '"' << json_escape(name) << "\":" << count;
        }
        out << "},\"actions\":{";
        first = true;
        for (const auto& [name, count] : actions) {
            if (!first)
                out << ',';
            first = false;
            out << '"' << json_escape(name) << "\":" << count;
        }
        out << "},\"evidence\":[";
        for (std::size_t i = 0; i < reasons.size(); ++i) {
            if (i)
                out << ',';
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
    for (const auto& [name, count] : diagnoses)
        out << "  " << name << ": " << count << " window(s)\n";
    out << "\nController actions:\n";
    for (const auto& [name, count] : actions)
        out << "  " << name << ": " << count << " window(s)\n";
    out << "\nEvidence and interpretation:\n";
    for (const auto& reason : reasons)
        out << "  - " << reason << "\n";
    out << "These are pipeline-stage observations and hypotheses; they do not prove a hardware-level cause.\n\n"
        << "Suggested reproducible follow-ups:\n"
        << "  1. Run the same dataset in a fresh repository with fixed workers 1, 2, 4, and 8.\n"
        << "  2. Compare a compressible text dataset with incompressible seeded random data.\n"
        << "  3. Repeat with a smaller memory budget and compare credit waits, queue fill, and completion time.\n";
    return out.str();
}

} // namespace pbe
