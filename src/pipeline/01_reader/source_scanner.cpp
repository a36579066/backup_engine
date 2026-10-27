#include "pipeline/01_reader/source_scanner.hpp"
#include "pipeline/shared/stage_support.hpp"

#include <algorithm>
#include <iostream>
#include <stdexcept>

namespace pbe::detail {
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
    std::filesystem::recursive_directory_iterator it(source,
                                                     std::filesystem::directory_options::none, ec),
        end;
    if (ec)
        throw std::runtime_error("cannot scan source: " + ec.message());
    for (; it != end;) {
        if (cancelled.load(std::memory_order_relaxed))
            throw std::runtime_error("backup cancelled during source scan");
        if (ec)
            throw std::runtime_error("source scan failed: " + ec.message());
        const auto path = it->path();
        (void)it->symlink_status(ec);
        if (ec)
            throw std::runtime_error("cannot stat source entry " + path.string() + ": " +
                                     ec.message());
        SourceEntry entry;
        entry.absolute = path;
        entry.relative = path.lexically_relative(source).generic_string();
        if (entry.relative.empty() || entry.relative == "." ||
            entry.relative.find('\0') != std::string::npos || entry.relative.starts_with("../"))
            throw std::runtime_error("source produced an invalid relative path");
        struct stat st{};

        // lstat describes the link itself; do not back up its target as though it were the link.
        if (::lstat(path.c_str(), &st) != 0)
            throw std::runtime_error(error_text("stat source entry " + path.string()));
        entry.mode = static_cast<std::uint32_t>(st.st_mode & 07777);
        entry.mtime_ns = stat_mtime_ns(st);
        entry.ctime_ns = stat_ctime_ns(st);
        entry.device = static_cast<std::uint64_t>(st.st_dev);
        entry.inode = static_cast<std::uint64_t>(st.st_ino);
        bool skipped = false;
        if (S_ISDIR(st.st_mode)) {
            entry.kind = "directory";
        } else if (S_ISREG(st.st_mode)) {
            entry.kind = "file";
            entry.size = static_cast<std::uint64_t>(st.st_size);
        } else if (S_ISLNK(st.st_mode)) {
            entry.kind = "symlink";
            entry.symlink_target = std::filesystem::read_symlink(path, ec).string();
            if (ec)
                throw std::runtime_error("cannot read symlink " + path.string() + ": " +
                                         ec.message());
        } else {
            if (skip_unsupported) {
                std::cerr << "warning: skipped unsupported source entry: " << path.string() << '\n';
                skipped = true;
            } else {
                throw std::runtime_error("unsupported source entry: " + path.string());
            }
        }
        if (!skipped)
            entries.push_back(std::move(entry));

        // An increment error may also produce end; check immediately after advancing the iterator.
        it.increment(ec);
        if (ec)
            throw std::runtime_error("source scan failed: " + ec.message());
    }

    // Sort entries for reproducible traversal and tests on an unchanged source tree.
    std::sort(entries.begin(), entries.end(), [](const auto& a, const auto& b) {
        return a.relative < b.relative;
    });
    return entries;
}
} // namespace pbe::detail
