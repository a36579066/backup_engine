#include "storage/object_store.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <limits>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

#include <zstd.h>

namespace pbe::detail {
namespace {
using Clock = std::chrono::steady_clock;
constexpr std::array<std::uint8_t, 8> kMagic{'P','B','E','O','B','J','1',0};

std::string error_text(std::string_view operation) {
    return std::string(operation) + ": " + std::strerror(errno);
}

void write_all(int fd, const std::uint8_t* data, std::size_t size) {
    std::size_t written = 0;
    while (written < size) {
        const ssize_t n = ::write(fd, data + written, size - written);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) throw std::runtime_error(error_text("write"));
        written += static_cast<std::size_t>(n);
    }
}

void read_all(int fd, std::uint8_t* data, std::size_t size) {
    std::size_t read_bytes = 0;
    while (read_bytes < size) {
        const ssize_t n = ::read(fd, data + read_bytes, size - read_bytes);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) throw std::runtime_error("unexpected end of file while reading repository data");
        read_bytes += static_cast<std::size_t>(n);
    }
}



void put_le(std::uint8_t* out, std::uint64_t value, std::size_t bytes) {
    for (std::size_t i = 0; i < bytes; ++i) out[i] = static_cast<std::uint8_t>(value >> (8 * i));
}

std::uint64_t get_le(const std::uint8_t* in, std::size_t bytes) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < bytes; ++i) value |= std::uint64_t(in[i]) << (8 * i);
    return value;
}

std::uint64_t elapsed_ns(Clock::time_point start) {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());
}

std::string random_id(std::string_view prefix) {
    std::random_device rd;
    std::mt19937_64 rng((std::uint64_t(rd()) << 32) ^ rd() ^
                        static_cast<std::uint64_t>(Clock::now().time_since_epoch().count()));
    const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    std::ostringstream out;
    out << prefix << now << '-' << std::hex << std::setfill('0') << std::setw(12) << (rng() & 0xffffffffffffull);
    return out.str();
}

void maybe_failpoint(const std::string& requested, std::string_view name) {
    if (!requested.empty() && requested == name) {
        std::cerr << "PBE fault injection: " << name << "\n";
        std::cerr.flush();
        std::_Exit(86);
    }
}




} // namespace

VerifiedObjectCache::VerifiedObjectCache(std::size_t capacity)
    : capacity_(std::max<std::size_t>(1, capacity)) {}

bool VerifiedObjectCache::contains_or_insert(const std::string& object_id) {
    std::lock_guard lock(mutex_);
    const auto found = positions_.find(object_id);
    if (found != positions_.end()) {
        recent_.splice(recent_.begin(), recent_, found->second);
        return true;
    }
    recent_.push_front(object_id);
    try {
        positions_.emplace(recent_.front(), recent_.begin());
    } catch (...) {
        recent_.pop_front();
        throw;
    }
    if (positions_.size() > capacity_) {
        const auto oldest = std::prev(recent_.end());
        positions_.erase(*oldest);
        recent_.erase(oldest);
    }
    return false;
}

std::size_t VerifiedObjectCache::size() const {
    std::lock_guard lock(mutex_);
    return positions_.size();
}

std::filesystem::path object_path(const std::filesystem::path& repo, const std::string& id) {
    if (id.size() != 64 || !std::all_of(id.begin(), id.end(), [](unsigned char c) {
            return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
        }))
        throw std::runtime_error("invalid object identifier in catalog");
    return repo / "objects" / id.substr(0, 2) / (id + ".obj");
}

ObjectHeader read_object_header(const std::filesystem::path& path) {
    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) throw std::runtime_error(error_text("open object " + path.string()));
    std::array<std::uint8_t, kObjectHeaderSize> bytes{};
    try { read_all(fd, bytes.data(), bytes.size()); }
    catch (...) { ::close(fd); throw; }
    struct stat st{};
    if (::fstat(fd, &st) != 0) { const auto e = error_text("stat object"); ::close(fd); throw std::runtime_error(e); }
    ::close(fd);
    if (!S_ISREG(st.st_mode)) throw std::runtime_error("object path is not a regular file: " + path.string());
    if (!std::equal(kMagic.begin(), kMagic.end(), bytes.begin())) throw std::runtime_error("object magic mismatch: " + path.string());
    if (get_le(bytes.data() + 8, 2) != 1) throw std::runtime_error("unsupported object version: " + path.string());
    const std::uint8_t codec = bytes[10];
    if (codec > 1) throw std::runtime_error("unsupported object codec: " + path.string());
    for (std::size_t i = 11; i < 16; ++i) if (bytes[i] != 0) throw std::runtime_error("object has unsupported flags");
    ObjectHeader header;
    header.codec = codec;
    header.raw_bytes = get_le(bytes.data() + 16, 8);
    header.stored_bytes = get_le(bytes.data() + 24, 8);
    std::copy_n(bytes.data() + 32, 32, header.digest.begin());
    if (header.raw_bytes > kChunkSize || header.stored_bytes > ZSTD_compressBound(kChunkSize)) throw std::runtime_error("object length exceeds format limits");
    if (static_cast<std::uint64_t>(st.st_size) != kObjectHeaderSize + header.stored_bytes) throw std::runtime_error("object file length does not match header");
    if (header.codec == 0 && header.raw_bytes != header.stored_bytes) throw std::runtime_error("raw object length mismatch");
    return header;
}





std::vector<std::uint8_t> read_object(const std::filesystem::path& path, const std::string& expected_id,
                                      ObjectReadTimings* timings) {
    const auto total_begin = Clock::now();
    const auto header_begin = Clock::now();
    const ObjectHeader header = read_object_header(path);
    if (timings) timings->io_ns += elapsed_ns(header_begin);
    if (digest_hex(header.digest) != expected_id) throw std::runtime_error("object filename and header digest differ");
    const auto read_begin = Clock::now();
    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) throw std::runtime_error(error_text("open object"));
    std::array<std::uint8_t, kObjectHeaderSize> ignored{};
    std::vector<std::uint8_t> payload(static_cast<std::size_t>(header.stored_bytes));
    try {
        read_all(fd, ignored.data(), ignored.size());
        if (!payload.empty()) read_all(fd, payload.data(), payload.size());
    } catch (...) { ::close(fd); throw; }
    ::close(fd);
    if (timings) timings->io_ns += elapsed_ns(read_begin);
    std::vector<std::uint8_t> raw(static_cast<std::size_t>(header.raw_bytes));
    if (header.codec == 0) raw = std::move(payload);
    else {
        const auto decompress_begin = Clock::now();
        const auto decoded = ZSTD_decompress(raw.data(), raw.size(), payload.data(), payload.size());
        if (ZSTD_isError(decoded) || decoded != raw.size()) throw std::runtime_error("zstd object decompression failed");
        if (timings) timings->decompression_ns += elapsed_ns(decompress_begin);
    }
    const auto hash_begin = Clock::now();
    if (blake3(raw) != header.digest) throw std::runtime_error("object content digest mismatch: " + expected_id);
    if (timings) {
        timings->hash_ns += elapsed_ns(hash_begin);
        timings->total_ns += elapsed_ns(total_begin);
    }
    return raw;
}

void fsync_directory(const std::filesystem::path& path) {
    int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) throw std::runtime_error(error_text("open directory for sync"));
    const int result = ::fsync(fd);
    const int saved = errno;
    ::close(fd);
    if (result != 0 && saved != EINVAL && saved != ENOTSUP) { errno = saved; throw std::runtime_error(error_text("sync directory")); }
}

WriteAck write_object(const std::filesystem::path& repo, const std::string& job,
                      BlobWriteTask& task, const std::string& failpoint,
                      ObjectWriteTimings* timings) {
    ObjectWriteTimings discarded_timings;
    ObjectWriteTimings* measured = timings ? timings : &discarded_timings;
    *measured = {};
    const auto total_begin = Clock::now();
    const auto setup_begin = Clock::now();
    const std::string id = digest_hex(task.digest);
    const auto dir = repo / "objects" / id.substr(0, 2);
    const bool shard_existed = std::filesystem::exists(dir);
    std::filesystem::create_directories(dir);
    const auto staging = repo / "staging" / job;
    std::filesystem::create_directories(staging);
    const auto temp = staging / (id + "." + random_id("obj-") + ".tmp");
    int fd = ::open(temp.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0) throw std::runtime_error(error_text("create staging object"));
    measured->setup_ns += elapsed_ns(setup_begin);
    std::array<std::uint8_t, kObjectHeaderSize> header{};
    std::copy(kMagic.begin(), kMagic.end(), header.begin());
    put_le(header.data() + 8, 1, 2);
    header[10] = task.codec;
    put_le(header.data() + 16, task.raw_bytes, 8);
    put_le(header.data() + 24, task.stored.size(), 8);
    std::copy(task.digest.begin(), task.digest.end(), header.begin() + 32);
    try {
        auto phase_begin = Clock::now();
        write_all(fd, header.data(), header.size());
        if (!task.stored.empty()) write_all(fd, task.stored.data(), task.stored.size());
        measured->staging_write_ns += elapsed_ns(phase_begin);
        maybe_failpoint(failpoint, "after-temp-write");
        phase_begin = Clock::now();
        const int sync_result = ::fsync(fd);
        const int sync_error = errno;
        measured->file_sync_ns += elapsed_ns(phase_begin);
        if (sync_result != 0) { errno = sync_error; throw std::runtime_error(error_text("sync staging object")); }
        phase_begin = Clock::now();
        const int close_result = ::close(fd);
        const int close_error = errno;
        measured->cleanup_ns += elapsed_ns(phase_begin);
        if (close_result != 0) { fd = -1; errno = close_error; throw std::runtime_error(error_text("close staging object")); }
        fd = -1;
        phase_begin = Clock::now();
        const int link_result = ::link(temp.c_str(), object_path(repo, id).c_str());
        const int link_error = errno;
        measured->publish_ns += elapsed_ns(phase_begin);
        if (link_result == 0) {
            maybe_failpoint(failpoint, "after-object-publish");
            phase_begin = Clock::now();
            const int unlink_result = ::unlink(temp.c_str());
            const int unlink_error = errno;
            measured->cleanup_ns += elapsed_ns(phase_begin);
            if (unlink_result != 0) { errno = unlink_error; throw std::runtime_error(error_text("remove published staging link")); }
            phase_begin = Clock::now();
            fsync_directory(dir);
            if (!shard_existed) fsync_directory(repo / "objects");
            measured->directory_sync_ns += elapsed_ns(phase_begin);
            measured->total_ns = elapsed_ns(total_begin);
            return WriteAck{ObjectHeader{task.codec, task.raw_bytes, task.stored.size(), task.digest, true, {}, 0}, true};
        }
        if (link_error != EEXIST) { errno = link_error; throw std::runtime_error(error_text("publish object")); }
        phase_begin = Clock::now();
        const int unlink_result = ::unlink(temp.c_str());
        const int unlink_error = errno;
        measured->cleanup_ns += elapsed_ns(phase_begin);
        if (unlink_result != 0) { errno = unlink_error; throw std::runtime_error(error_text("remove duplicate staging object")); }
        phase_begin = Clock::now();
        const ObjectHeader existing = read_object_header(object_path(repo, id));
        measured->existing_check_ns += elapsed_ns(phase_begin);
        if (existing.digest != task.digest || existing.raw_bytes != task.raw_bytes) throw std::runtime_error("existing object conflicts with its content address");
        measured->total_ns = elapsed_ns(total_begin);
        return WriteAck{existing, false};
    } catch (...) {
        const auto cleanup_begin = Clock::now();
        if (fd >= 0) ::close(fd);
        ::unlink(temp.c_str());
        measured->cleanup_ns += elapsed_ns(cleanup_begin);
        measured->total_ns = elapsed_ns(total_begin);
        throw;
    }
}

} // namespace pbe::detail
