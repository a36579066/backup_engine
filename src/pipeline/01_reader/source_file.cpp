#include "pipeline/01_reader/source_file.hpp"
#include "pipeline/shared/stage_support.hpp"

#include <cerrno>
#include <fcntl.h>
#include <stdexcept>
#include <unistd.h>

namespace pbe::detail {
SourceFile::SourceFile(const SourceEntry& entry)
    : fd_(-1),
      relative_(entry.relative) {
    // O_NOFOLLOW rejects symlink replacement; fstat verifies the opened file still matches the scan.
    fd_ = ::open(entry.absolute.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd_ < 0)
        throw std::runtime_error(error_text("open source file " + entry.absolute.string()));
    try {
        if (::fstat(fd_, &before_) != 0)
            throw std::runtime_error(error_text("stat open source file"));
        if (!S_ISREG(before_.st_mode) ||
            static_cast<std::uint64_t>(before_.st_size) != entry.size ||
            stat_mtime_ns(before_) != entry.mtime_ns || stat_ctime_ns(before_) != entry.ctime_ns ||
            static_cast<std::uint64_t>(before_.st_dev) != entry.device ||
            static_cast<std::uint64_t>(before_.st_ino) != entry.inode ||
            static_cast<std::uint32_t>(before_.st_mode & 07777) != entry.mode)
            throw std::runtime_error("source changed between scan and read: " + entry.relative);
    } catch (...) {
        ::close(fd_);
        throw;
    }
}

SourceFile::~SourceFile() {
    ::close(fd_);
}

void SourceFile::read_into(std::span<std::uint8_t> bytes) {
    std::size_t count = 0;
    while (count < bytes.size()) {
        const auto n = ::read(fd_, bytes.data() + count, bytes.size() - count);

        // Handle partial reads and EINTR until the chunk buffer is filled.
        if (n < 0 && errno == EINTR)
            continue;
        if (n <= 0)
            throw std::runtime_error("source file became shorter while reading: " + relative_);
        count += static_cast<std::size_t>(n);
    }
}

void SourceFile::validate_unchanged() const {
    struct stat after{};
    if (::fstat(fd_, &after) != 0)
        throw std::runtime_error(error_text("stat source file after read"));
    if (before_.st_dev != after.st_dev || before_.st_ino != after.st_ino ||
        before_.st_size != after.st_size || stat_mtime_ns(before_) != stat_mtime_ns(after) ||
        stat_ctime_ns(before_) != stat_ctime_ns(after))
        throw std::runtime_error("source changed while being read: " + relative_);
}
} // namespace pbe::detail
