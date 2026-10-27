#include "pbe/engine.hpp"

#include "storage/catalog.hpp"
#include "storage/object_store.hpp"
#include "storage/repository.hpp"
#include "storage/snapshot_catalog.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <stdexcept>
#include <string_view>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace pbe {
namespace {
using detail::Catalog;
using detail::DbEntry;
using detail::canonical_existing;
using detail::ensure_chunk_layout;
using detail::load_chunks;
using detail::load_entries;
using detail::object_path;
using detail::path_parts;
using detail::read_object;
using detail::require_complete;
using detail::validate_repo;
constexpr auto kChunkSize = detail::kChunkSize;

std::string error_text(std::string_view operation) {
    return std::string(operation) + ": " + std::strerror(errno);
}

void write_all(int fd, const std::uint8_t* data, std::size_t size) {
    std::size_t written = 0;
    while (written < size) {
        const ssize_t n = ::write(fd, data + written, size - written);
        if (n < 0 && errno == EINTR)
            continue;
        if (n <= 0)
            throw std::runtime_error(error_text("write restored file"));
        written += static_cast<std::size_t>(n);
    }
}

timespec nanoseconds_to_timespec(std::int64_t value) {
    std::int64_t seconds = value / 1000000000ll;
    std::int64_t nanos = value % 1000000000ll;
    if (nanos < 0) {
        --seconds;
        nanos += 1000000000ll;
    }
    return timespec{static_cast<time_t>(seconds), static_cast<long>(nanos)};
}

int open_directory_path(int root_fd, const std::vector<std::string>& parts, std::size_t count) {
    // Traverse relative to open directory descriptors and reject symlinks to keep restores inside the target.
    int current = ::dup(root_fd);
    if (current < 0)
        throw std::runtime_error(error_text("duplicate restore root descriptor"));
    for (std::size_t i = 0; i < count; ++i) {
        int next =
            ::openat(current, parts[i].c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
        if (next < 0) {
            const auto e = error_text("open restore directory component " + parts[i]);
            ::close(current);
            throw std::runtime_error(e);
        }
        ::close(current);
        current = next;
    }
    return current;
}

std::string random_temp_name() {
    static std::atomic<std::uint64_t> sequence{0};
    return ".pbe-restore-" + std::to_string(sequence.fetch_add(1, std::memory_order_relaxed)) +
           "-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
           ".tmp";
}

void create_file_at(int parent_fd, const std::string& name, sqlite3* db,
                    const std::filesystem::path& repo, const std::string& snapshot,
                    const DbEntry& entry, bool packed_storage) {
    const std::string temp = random_temp_name();
    int fd = ::openat(parent_fd, temp.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
                      0600);
    if (fd < 0)
        throw std::runtime_error(error_text("create restore temporary file"));
    try {
        std::uint64_t total = 0;

        // Read, verify, and write one chunk at a time instead of loading whole files into memory.
        for (const auto& ref : load_chunks(db, snapshot, entry.path, packed_storage)) {
            if (ref.ordinal != total / kChunkSize || ref.bytes > kChunkSize)
                throw std::runtime_error("invalid chunk ordering or length in catalog");
            auto raw = packed_storage
                           ? pbe::detail::read_packed_object(repo, ref.location, ref.object_id)
                           : read_object(object_path(repo, ref.object_id), ref.object_id);
            if (raw.size() != ref.bytes)
                throw std::runtime_error("chunk size differs from catalog");
            write_all(fd, raw.data(), raw.size());
            total += raw.size();
        }
        if (total != entry.size)
            throw std::runtime_error("restored file size differs from snapshot metadata");
        if (::fchmod(fd, entry.mode & 07777) != 0)
            throw std::runtime_error(error_text("set restored file mode"));
        const timespec file_time = nanoseconds_to_timespec(entry.mtime_ns);
        const timespec times[2]{{0, UTIME_OMIT}, file_time};
        if (::futimens(fd, times) != 0)
            throw std::runtime_error(error_text("set restored file time"));
        if (::fsync(fd) != 0)
            throw std::runtime_error(error_text("sync restored file"));
        if (::close(fd) != 0) {
            fd = -1;
            throw std::runtime_error(error_text("close restored file"));
        }
        fd = -1;

        // Write a temporary file completely, then publish it without replacing an existing path.
        if (::linkat(parent_fd, temp.c_str(), parent_fd, name.c_str(), 0) != 0)
            throw std::runtime_error(errno == EEXIST
                                         ? "restore would overwrite an existing path: " + entry.path
                                         : error_text("publish restored file"));
        if (::unlinkat(parent_fd, temp.c_str(), 0) != 0)
            throw std::runtime_error(error_text("remove restored temporary file"));
    } catch (...) {
        if (fd >= 0)
            ::close(fd);
        ::unlinkat(parent_fd, temp.c_str(), 0);
        throw;
    }
}
} // namespace

void restore_snapshot(const RestoreOptions& options) {
    // Restore runs serially on the caller thread; backup CPU/Storage workers are not used here.
    if (options.target.empty())
        throw std::runtime_error("restore requires --target");
    validate_repo(options.repo);
    const auto repo = canonical_existing(options.repo);
    const bool packed_storage = pbe::detail::repository_format_version(repo) == 2;
    Catalog catalog(repo / "catalog.sqlite");

    // 1. Require a COMPLETE snapshot and validate paths and chunk layout before restoring.
    require_complete(catalog.get(), options.snapshot);
    const auto entries = load_entries(catalog.get(), options.snapshot);
    for (const auto& entry : entries)
        (void)path_parts(entry.path);
    ensure_chunk_layout(catalog.get(), options.snapshot);

    // 2. Require an empty target directory before creating restored content.
    auto target = std::filesystem::absolute(options.target).lexically_normal();
    std::error_code ec;
    std::filesystem::create_directories(target.parent_path(), ec);
    if (ec)
        throw std::runtime_error("cannot create restore parent directory: " + ec.message());
    struct stat target_stat{};
    bool target_exists = (::lstat(target.c_str(), &target_stat) == 0);
    if (target_exists) {
        if (!S_ISDIR(target_stat.st_mode) || S_ISLNK(target_stat.st_mode))
            throw std::runtime_error("restore target must be a real directory");
        if (std::filesystem::directory_iterator(target) != std::filesystem::directory_iterator{})
            throw std::runtime_error("restore target must be empty");
    } else if (errno != ENOENT)
        throw std::runtime_error(error_text("inspect restore target"));
    else if (!std::filesystem::create_directory(target, ec) || ec)
        throw std::runtime_error("cannot create restore target: " + ec.message());

    const int root_fd = ::open(target.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (root_fd < 0)
        throw std::runtime_error(error_text("open restore target"));
    try {
        std::vector<DbEntry> directories, files, symlinks;
        for (const auto& entry : entries) {
            if (entry.kind == "directory")
                directories.push_back(entry);
            else if (entry.kind == "file")
                files.push_back(entry);
            else if (entry.kind == "symlink")
                symlinks.push_back(entry);
            else
                throw std::runtime_error("unknown entry kind in snapshot");
        }

        // 3. Create parent directories first, then regular files, then symlinks.
        std::sort(directories.begin(), directories.end(), [](const auto& a, const auto& b) {
            const auto da = std::count(a.path.begin(), a.path.end(), '/');
            const auto db = std::count(b.path.begin(), b.path.end(), '/');
            return da == db ? a.path < b.path : da < db;
        });
        for (const auto& entry : directories) {
            const auto parts = path_parts(entry.path);
            int parent = open_directory_path(root_fd, parts, parts.size() - 1);
            if (::mkdirat(parent, parts.back().c_str(), 0700) != 0) {
                const auto e = error_text("create restored directory " + entry.path);
                ::close(parent);
                throw std::runtime_error(e);
            }
            ::close(parent);
        }
        for (const auto& entry : files) {
            const auto parts = path_parts(entry.path);
            int parent = open_directory_path(root_fd, parts, parts.size() - 1);
            try {
                create_file_at(parent, parts.back(), catalog.get(), repo, options.snapshot, entry,
                               packed_storage);
            } catch (...) {
                ::close(parent);
                throw;
            }
            ::close(parent);
        }
        for (const auto& entry : symlinks) {
            if (entry.symlink_target.find('\0') != std::string::npos)
                throw std::runtime_error("invalid symlink target in catalog");
            const auto parts = path_parts(entry.path);
            int parent = open_directory_path(root_fd, parts, parts.size() - 1);
            if (::symlinkat(entry.symlink_target.c_str(), parent, parts.back().c_str()) != 0) {
                const auto e = error_text("restore symlink " + entry.path);
                ::close(parent);
                throw std::runtime_error(e);
            }
            const timespec symlink_time = nanoseconds_to_timespec(entry.mtime_ns);
            const timespec times[2]{{0, UTIME_OMIT}, symlink_time};
            (void)::utimensat(parent, parts.back().c_str(), times, AT_SYMLINK_NOFOLLOW);
            ::close(parent);
        }

        // 4. Restore directory modes and mtimes deepest-first after creating their contents.
        for (auto it = directories.rbegin(); it != directories.rend(); ++it) {
            const auto parts = path_parts(it->path);
            int directory_fd = open_directory_path(root_fd, parts, parts.size());
            if (::fchmod(directory_fd, it->mode & 07777) != 0) {
                const auto e = error_text("set restored directory mode");
                ::close(directory_fd);
                throw std::runtime_error(e);
            }
            const timespec directory_time = nanoseconds_to_timespec(it->mtime_ns);
            const timespec times[2]{{0, UTIME_OMIT}, directory_time};
            if (::futimens(directory_fd, times) != 0) {
                const auto e = error_text("set restored directory time");
                ::close(directory_fd);
                throw std::runtime_error(e);
            }
            ::close(directory_fd);
        }
        if (::fsync(root_fd) != 0)
            throw std::runtime_error(error_text("sync restored directory"));
    } catch (...) {
        ::close(root_fd);
        throw;
    }
    ::close(root_fd);
}
} // namespace pbe
