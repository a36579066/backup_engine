#include "io_call_metrics.hpp"
#include "pbe/bounded_queue.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <exception>
#include <fcntl.h>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <stdio.h>
#include <sys/stat.h>
#include <thread>

namespace {
namespace fs = std::filesystem;
using pbe::profiling::Clock;
using pbe::profiling::IoCalls;
using pbe::profiling::measure;
constexpr std::size_t kBufferBytes = 8u << 20;
constexpr std::size_t kBufferCount = 16;
constexpr std::size_t kSegmentBytes = 128u << 20;

std::runtime_error error(const char* action) {
    return std::runtime_error(std::string(action) + ": " + std::strerror(errno));
}

struct Input {
    fs::path path;
    std::uint64_t size;
};

std::vector<Input> list_files(const fs::path& source) {
    std::vector<Input> files;
    for (const auto& entry : fs::recursive_directory_iterator(source)) {
        const auto status = entry.symlink_status();
        if (fs::is_directory(status))
            continue;
        if (!fs::is_regular_file(status))
            throw std::runtime_error("input must contain regular files");
        files.push_back({entry.path(), entry.file_size()});
    }
    std::sort(files.begin(), files.end(), [](const Input& a, const Input& b) {
        return a.path < b.path;
    });
    return files;
}

// Independent POSIX input stream fills reusable buffers across files without engine stages.
class InputStream {
public:
    InputStream(const std::vector<Input>& files, IoCalls* metrics = nullptr)
        : files_(files),
          metrics_(metrics) {
    }

    ~InputStream() {
        if (fd_ >= 0)
            ::close(fd_);
    }

    std::size_t fill(std::span<std::uint8_t> buffer) {
        std::size_t used = 0;
        while (used < buffer.size()) {
            if (fd_ < 0) {
                if (next_ == files_.size())
                    break;
                fd_ = ::open(files_[next_].path.c_str(), O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
                if (fd_ < 0 || ::fstat(fd_, &before_) != 0)
                    throw error("open/stat input");
                if (!S_ISREG(before_.st_mode) ||
                    static_cast<std::uint64_t>(before_.st_size) != files_[next_].size)
                    throw std::runtime_error("input changed before read");
                remaining_ = files_[next_++].size;
            }
            if (!remaining_) {
                close_checked();
                continue;
            }
            const auto want = std::min<std::uint64_t>(remaining_, buffer.size() - used);
            const auto read = [&] {
                return ::read(fd_, buffer.data() + used, want);
            };
            const auto n = metrics_ ? measure(*metrics_, read) : read();
            if (n < 0 && errno == EINTR)
                continue;
            if (n <= 0)
                throw error("read input");
            used += n;
            remaining_ -= n;
        }
        // Validate the final file even when its last byte exactly fills the buffer.
        if (fd_ >= 0 && !remaining_)
            close_checked();
        return used;
    }

private:
    void close_checked() {
        struct stat after{};
        if (::fstat(fd_, &after) != 0)
            throw error("stat input after read");
        if (after.st_size != before_.st_size || after.st_ino != before_.st_ino ||
            after.st_dev != before_.st_dev ||
            after.st_mtimespec.tv_sec != before_.st_mtimespec.tv_sec ||
            after.st_mtimespec.tv_nsec != before_.st_mtimespec.tv_nsec ||
            after.st_ctimespec.tv_sec != before_.st_ctimespec.tv_sec ||
            after.st_ctimespec.tv_nsec != before_.st_ctimespec.tv_nsec)
            throw std::runtime_error("input changed during read");
        ::close(fd_);
        fd_ = -1;
    }

    const std::vector<Input>& files_;
    IoCalls* metrics_;
    std::size_t next_ = 0;
    std::uint64_t remaining_ = 0;
    int fd_ = -1;
    struct stat before_{};
};

void sync_directory(const fs::path& path, IoCalls* metrics = nullptr) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0)
        throw error("open output directory");
    const auto sync = [&] {
        return ::fsync(fd);
    };
    const int result = metrics ? measure(*metrics, sync) : sync();
    const int saved = errno;
    ::close(fd);
    if (result != 0 && saved != EINVAL && saved != ENOTSUP) {
        errno = saved;
        throw error("sync output directory");
    }
}

// 128 MiB segments with file sync, no-replace publication, and directory sync; independent of PackWriter.
class OutputStream {
public:
    OutputStream(const fs::path& root, bool exclusive_rename)
        : root_(root),
          exclusive_rename_(exclusive_rename) {
        fs::create_directories(root_ / "staging");
        fs::create_directories(root_ / "objects");
    }

    ~OutputStream() {
        if (fd_ >= 0)
            ::close(fd_);
        if (!temp_.empty()) {
            std::error_code ec;
            fs::remove(temp_, ec);
        }
    }

    void append(std::span<const std::uint8_t> bytes) {
        std::size_t offset = 0;
        while (offset < bytes.size()) {
            if (fd_ < 0)
                open_segment();
            const auto want = std::min(bytes.size() - offset, kSegmentBytes - size_);
            const auto n = measure(writes, [&] {
                return ::write(fd_, bytes.data() + offset, want);
            });
            if (n < 0 && errno == EINTR)
                continue;
            if (n <= 0)
                throw error("write output");
            size_ += n;
            offset += n;
            if (size_ == kSegmentBytes)
                publish();
        }
    }

    void finish() {
        if (fd_ >= 0)
            publish();
    }

    IoCalls writes, syncs, publications, directory_syncs;
    std::size_t linked_segments = 0;
    std::size_t renamed_segments = 0;
    std::size_t unsupported_link_fallbacks = 0;
    std::vector<Input> files;

private:
    void open_segment() {
        temp_ = root_ / "staging" / (std::to_string(files.size()) + ".tmp");
        fd_ = ::open(temp_.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (fd_ < 0)
            throw error("create output segment");
        size_ = 0;
    }

    void publish() {
        if (measure(syncs, [&] {
                return ::fsync(fd_);
            }) != 0)
            throw error("sync output segment");
        const int closed = ::close(fd_);
        fd_ = -1;
        if (closed != 0)
            throw error("close output segment");
        const auto path = root_ / "objects" / (std::to_string(files.size()) + ".bin");
        publish_without_replacement(path);
        temp_.clear();
        sync_directory(root_ / "objects", &directory_syncs);
        files.push_back({path, size_});
    }

    void publish_without_replacement(const fs::path& path) {
        if (!exclusive_rename_) {
            const int result = measure(publications, [&] {
                return ::link(temp_.c_str(), path.c_str());
            });
            if (result == 0) {
                ++linked_segments;
                if (::unlink(temp_.c_str()) != 0)
                    throw error("unlink output staging name");
                return;
            }
            if (errno != ENOTSUP && errno != EOPNOTSUPP && errno != ENOSYS)
                throw error("publish output segment with link");
            ++unsupported_link_fallbacks;
        }

        // Keep the control independent of the engine's publication implementation.
        // RENAME_EXCL permits FAT32 testing without replacing an existing destination.
        if (measure(publications, [&] {
                return ::renamex_np(temp_.c_str(), path.c_str(), RENAME_EXCL);
            }) != 0)
            throw error("publish output segment with exclusive rename");
        ++renamed_segments;
    }

    fs::path root_, temp_;
    const bool exclusive_rename_;
    int fd_ = -1;
    std::size_t size_ = 0;
};

struct Block {
    std::vector<std::uint8_t> data = std::vector<std::uint8_t>(kBufferBytes);
    std::size_t used = 0, seed = 0;
};

struct Token {
    std::size_t seed, used;
};

struct RunStats {
    std::uint64_t bytes = 0;
    double producer_queue_wait = 0, writer_queue_wait = 0;
    std::vector<Token> tokens;
};

RunStats transfer(const std::string& mode, const std::vector<Input>& files, std::uint64_t total,
                  IoCalls& reads, OutputStream& output,
                  pbe::BoundedQueue<std::unique_ptr<Block>>& free) {
    pbe::BoundedQueue<std::unique_ptr<Block>> ready(kBufferCount);
    std::atomic<bool> cancelled{false};
    std::mutex error_mutex;
    std::exception_ptr failure;
    const auto fail = [&](std::exception_ptr error) {
        {
            std::lock_guard lock(error_mutex);
            if (!failure)
                failure = error;
        }
        cancelled.store(true);
        free.close();
        ready.close();
    };
    RunStats stats;
    stats.tokens.reserve(total / kBufferBytes + 1);
    std::thread writer([&] {
        try {
            for (;;) {
                const auto begin = Clock::now();
                auto block = ready.pop(&cancelled);
                stats.writer_queue_wait +=
                    std::chrono::duration<double>(Clock::now() - begin).count();
                if (!block)
                    break;
                if (cancelled.load())
                    break;
                output.append(std::span((*block)->data.data(), (*block)->used));
                stats.tokens.push_back({(*block)->seed, (*block)->used});
                if (!free.push(std::move(*block), &cancelled))
                    break;
            }
            if (!cancelled.load())
                output.finish();
        } catch (...) {
            fail(std::current_exception());
        }
    });
    try {
        InputStream source(files, &reads);
        while (stats.bytes < total && !cancelled.load()) {
            const auto begin = Clock::now();
            auto block = free.pop(&cancelled);
            stats.producer_queue_wait +=
                std::chrono::duration<double>(Clock::now() - begin).count();
            if (!block || cancelled.load())
                break;
            const auto want = std::min<std::uint64_t>(kBufferBytes, total - stats.bytes);
            (*block)->used =
                mode == "copy" ? source.fill(std::span((*block)->data.data(), want)) : want;
            if ((*block)->used != want)
                throw std::runtime_error("input byte count mismatch");
            stats.bytes += want;
            if (!ready.push(std::move(*block), &cancelled))
                break;
        }
    } catch (...) {
        fail(std::current_exception());
    }
    ready.close();
    writer.join();
    if (failure)
        std::rethrow_exception(failure);
    if (stats.bytes != total)
        throw std::runtime_error("native transfer was incomplete");
    return stats;
}

void verify(const std::string& mode, const std::vector<Input>& source, const OutputStream& output,
            const std::vector<Block*>& seeds, const RunStats& stats) {
    InputStream stored(output.files), original(source);
    std::vector<std::uint8_t> actual(kBufferBytes), expected(kBufferBytes);
    for (const auto& token : stats.tokens) {
        if (stored.fill(std::span(actual.data(), token.used)) != token.used)
            throw std::runtime_error("native verification: truncated output");
        const std::uint8_t* reference = nullptr;
        if (mode == "copy") {
            if (original.fill(std::span(expected.data(), token.used)) != token.used)
                throw std::runtime_error("native verification: truncated source");
            reference = expected.data();
        } else
            reference = seeds.at(token.seed)->data.data();
        if (!std::equal(actual.data(), actual.data() + token.used, reference))
            throw std::runtime_error("native verification: content mismatch");
    }
}

void run(const std::string& mode, const fs::path& source, const fs::path& destination,
         bool exclusive_rename) {
    const auto files = list_files(source);
    std::uint64_t total = 0;
    for (const auto& file : files)
        total += file.size;
    if (!total)
        throw std::runtime_error("native probe requires a nonempty source");
    if (fs::exists(destination) && (!fs::is_directory(destination) || !fs::is_empty(destination)))
        throw std::runtime_error("output must be a new or empty directory");
    OutputStream output(destination, exclusive_rename);
    IoCalls reads;
    pbe::BoundedQueue<std::unique_ptr<Block>> free(kBufferCount);
    std::vector<Block*> seeds;
    if (mode != "read") {
        InputStream seed_source(files);
        for (std::size_t i = 0; i < kBufferCount; ++i) {
            auto block = std::make_unique<Block>();
            block->seed = i;
            if (mode == "memory-write" && seed_source.fill(block->data) != kBufferBytes)
                throw std::runtime_error(
                    "memory-write requires at least 128 MiB of source seed data");
            seeds.push_back(block.get());
            free.push(std::move(block));
        }
    }
    std::vector<std::uint8_t> read_buffer(mode == "read" ? kBufferBytes : 0);
    const auto process_before = pbe::profiling::ProcessIo::read();
    rusage before{}, after{};
    ::getrusage(RUSAGE_SELF, &before);
    const auto started_unix =
        std::chrono::duration<double>(std::chrono::system_clock::now().time_since_epoch()).count();
    const auto begin = Clock::now();
    RunStats stats;
    if (mode == "read") {
        InputStream input(files, &reads);
        while (auto n = input.fill(read_buffer))
            stats.bytes += n;
        if (stats.bytes != total)
            throw std::runtime_error("read-only byte count mismatch");
    } else
        stats = transfer(mode, files, total, reads, output, free);
    const double seconds = std::chrono::duration<double>(Clock::now() - begin).count();
    const auto finished_unix =
        std::chrono::duration<double>(std::chrono::system_clock::now().time_since_epoch()).count();
    ::getrusage(RUSAGE_SELF, &after);
    const auto process_after = pbe::profiling::ProcessIo::read();
    const auto verify_begin = Clock::now();
    if (mode != "read")
        verify(mode, files, output, seeds, stats);
    const auto verify_seconds = std::chrono::duration<double>(Clock::now() - verify_begin).count();
    const auto cpu = [](const timeval& t) {
        return t.tv_sec + t.tv_usec / 1e6;
    };
    std::cout << std::setprecision(12) << "{\"mode\":\"" << mode
              << "\",\"elapsed_seconds\":" << seconds << ",\"verify_seconds\":" << verify_seconds
              << ",\"started_unix\":" << started_unix << ",\"finished_unix\":" << finished_unix
              << ",\"verified\":true,\"logical_bytes\":" << total
              << ",\"segments\":" << output.files.size() << ",\"publication_requested\":\""
              << (exclusive_rename ? "exclusive-rename" : "auto")
              << "\",\"linked_segments\":" << output.linked_segments
              << ",\"renamed_segments\":" << output.renamed_segments
              << ",\"unsupported_link_fallbacks\":" << output.unsupported_link_fallbacks
              << ",\"user_cpu_seconds\":" << cpu(after.ru_utime) - cpu(before.ru_utime)
              << ",\"system_cpu_seconds\":" << cpu(after.ru_stime) - cpu(before.ru_stime)
              << ",\"voluntary_switches\":" << after.ru_nvcsw - before.ru_nvcsw
              << ",\"involuntary_switches\":" << after.ru_nivcsw - before.ru_nivcsw
              << ",\"producer_queue_wait\":" << stats.producer_queue_wait
              << ",\"writer_queue_wait\":" << stats.writer_queue_wait << ",\"process_io\":";
    pbe::profiling::process_io_json(std::cout, process_before, process_after);
    std::cout << ",\"read\":";
    reads.json(std::cout);
    std::cout << ",\"write\":";
    output.writes.json(std::cout);
    std::cout << ",\"fsync\":";
    output.syncs.json(std::cout);
    std::cout << ",\"publication\":";
    output.publications.json(std::cout);
    std::cout << ",\"directory_sync\":";
    output.directory_syncs.json(std::cout);
    std::cout << "}\n";
}
} // namespace

int main(int argc, char** argv) {
    try {
        if (argc != 4 && argc != 5)
            throw std::runtime_error(
                "usage: pbe_native_io read|copy|memory-write SOURCE EMPTY_OUTPUT [auto|exclusive-rename]");
        const std::string mode = argv[1];
        if (mode != "read" && mode != "copy" && mode != "memory-write")
            throw std::runtime_error("unknown native I/O mode");
        const std::string publication = argc == 5 ? argv[4] : "auto";
        if (publication != "auto" && publication != "exclusive-rename")
            throw std::runtime_error("unknown publication mode");
        run(mode, argv[2], argv[3], publication == "exclusive-rename");
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "pbe_native_io: " << e.what() << '\n';
        return 1;
    }
}
