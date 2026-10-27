#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <string>
#include <sys/stat.h>
#include <vector>

namespace pbe::detail {
// Scan metadata is later checked against device/inode and timestamps to detect source changes.
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

std::int64_t stat_mtime_ns(const struct stat& st);
std::int64_t stat_ctime_ns(const struct stat& st);
std::vector<SourceEntry> scan_source(const std::filesystem::path& source, bool skip_unsupported,
                                     const std::atomic<bool>& cancelled);
} // namespace pbe::detail
