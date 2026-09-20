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
constexpr std::array<std::uint8_t, 8> kPackMagic{'P','B','E','P','A','C','K','2'};
constexpr std::size_t kPackHeaderSize = 16;

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

void pread_all(int fd, std::uint8_t* data, std::size_t size, std::uint64_t offset) {
    if (offset > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max()))
        throw std::runtime_error("object offset exceeds platform limits");
    std::size_t read_bytes = 0;
    while (read_bytes < size) {
        const auto current = offset + read_bytes;
        if (current > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max()))
            throw std::runtime_error("object offset exceeds platform limits");
        const ssize_t n = ::pread(fd, data + read_bytes, size - read_bytes, static_cast<off_t>(current));
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

std::filesystem::path pack_path(const std::filesystem::path& repo, const std::string& pack_id) {
    if (pack_id.empty() || !std::all_of(pack_id.begin(), pack_id.end(), [](unsigned char c) {
            return std::isalnum(c) || c == '-';
        }))
        throw std::runtime_error("invalid pack identifier in catalog");
    return repo / "objects" / "packs" / (pack_id + ".pack");
}

void validate_pack_header(int fd, const std::filesystem::path& path) {
    std::array<std::uint8_t, kPackHeaderSize> bytes{};
    pread_all(fd, bytes.data(), bytes.size(), 0);
    if (!std::equal(kPackMagic.begin(), kPackMagic.end(), bytes.begin()) || get_le(bytes.data() + 8, 2) != 1)
        throw std::runtime_error("pack header mismatch: " + path.string());
    for (std::size_t i = 10; i < bytes.size(); ++i)
        if (bytes[i] != 0) throw std::runtime_error("pack has unsupported flags: " + path.string());
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

ObjectHeader read_packed_object_header(const std::filesystem::path& repo, const ObjectLocation& location) {
    const auto path = pack_path(repo, location.pack_id);
    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) throw std::runtime_error(error_text("open object pack " + path.string()));
    struct stat st{};
    if (::fstat(fd, &st) != 0) { const auto e = error_text("stat object pack"); ::close(fd); throw std::runtime_error(e); }
    try {
        if (!S_ISREG(st.st_mode) || st.st_size < static_cast<off_t>(kPackHeaderSize))
            throw std::runtime_error("pack path is not a valid regular file: " + path.string());
        validate_pack_header(fd, path);
        if (location.pack_offset < kPackHeaderSize || location.pack_offset > static_cast<std::uint64_t>(st.st_size) ||
            static_cast<std::uint64_t>(st.st_size) - location.pack_offset < kObjectHeaderSize)
            throw std::runtime_error("object offset is outside its pack: " + path.string());
        std::array<std::uint8_t, kObjectHeaderSize> bytes{};
        pread_all(fd, bytes.data(), bytes.size(), location.pack_offset);
        if (!std::equal(kMagic.begin(), kMagic.end(), bytes.begin())) throw std::runtime_error("object magic mismatch in pack: " + path.string());
        if (get_le(bytes.data() + 8, 2) != 1) throw std::runtime_error("unsupported packed object version");
        const std::uint8_t codec = bytes[10];
        if (codec > 1) throw std::runtime_error("unsupported packed object codec");
        for (std::size_t i = 11; i < 16; ++i) if (bytes[i] != 0) throw std::runtime_error("packed object has unsupported flags");
        ObjectHeader header;
        header.codec = codec;
        header.raw_bytes = get_le(bytes.data() + 16, 8);
        header.stored_bytes = get_le(bytes.data() + 24, 8);
        std::copy_n(bytes.data() + 32, 32, header.digest.begin());
        header.pack_id = location.pack_id;
        header.pack_offset = location.pack_offset;
        if (header.raw_bytes > kChunkSize || header.stored_bytes > ZSTD_compressBound(kChunkSize))
            throw std::runtime_error("packed object length exceeds format limits");
        const std::uint64_t available = static_cast<std::uint64_t>(st.st_size) - location.pack_offset - kObjectHeaderSize;
        if (header.stored_bytes > available) throw std::runtime_error("packed object payload exceeds pack length");
        if (header.codec == 0 && header.raw_bytes != header.stored_bytes) throw std::runtime_error("packed raw object length mismatch");
        ::close(fd);
        return header;
    } catch (...) {
        ::close(fd);
        throw;
    }
}

std::vector<std::uint8_t> read_packed_object(const std::filesystem::path& repo, const ObjectLocation& location,
                                             const std::string& expected_id, ObjectReadTimings* timings) {
    const auto total_begin = Clock::now();
    const auto header_begin = Clock::now();
    const ObjectHeader header = read_packed_object_header(repo, location);
    if (timings) timings->io_ns += elapsed_ns(header_begin);
    if (digest_hex(header.digest) != expected_id) throw std::runtime_error("pack index and object digest differ");
    const auto path = pack_path(repo, location.pack_id);
    const auto read_begin = Clock::now();
    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) throw std::runtime_error(error_text("open object pack"));
    std::vector<std::uint8_t> payload(static_cast<std::size_t>(header.stored_bytes));
    try {
        if (!payload.empty()) pread_all(fd, payload.data(), payload.size(), location.pack_offset + kObjectHeaderSize);
    } catch (...) { ::close(fd); throw; }
    ::close(fd);
    if (timings) timings->io_ns += elapsed_ns(read_begin);
    std::vector<std::uint8_t> raw(static_cast<std::size_t>(header.raw_bytes));
    if (header.codec == 0) raw = std::move(payload);
    else {
        const auto decompress_begin = Clock::now();
        const auto decoded = ZSTD_decompress(raw.data(), raw.size(), payload.data(), payload.size());
        if (ZSTD_isError(decoded) || decoded != raw.size()) throw std::runtime_error("zstd packed object decompression failed");
        if (timings) timings->decompression_ns += elapsed_ns(decompress_begin);
    }
    const auto hash_begin = Clock::now();
    if (blake3(raw) != header.digest) throw std::runtime_error("packed object content digest mismatch: " + expected_id);
    if (timings) {
        timings->hash_ns += elapsed_ns(hash_begin);
        timings->total_ns += elapsed_ns(total_begin);
    }
    return raw;
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

struct BatchPackWriter {
    std::filesystem::path objects_dir;
    std::filesystem::path packs_dir;
    std::filesystem::path temp;
    std::filesystem::path published;
    std::string pack_id;
    std::string failpoint;
    bool packs_dir_existed = false;
    bool temp_exists = false;
    bool published_successfully = false;
    int fd = -1;
    std::uint64_t bytes = kPackHeaderSize;
    std::size_t objects = 0;
    ObjectWriteTimings measured;

    BatchPackWriter(const std::filesystem::path& repo, const std::string& job, const std::string& requested_failpoint)
        : objects_dir(repo / "objects"), packs_dir(objects_dir / "packs"), failpoint(requested_failpoint) {
        const auto operation_begin = Clock::now();
        const auto setup_begin = Clock::now();
        packs_dir_existed = std::filesystem::exists(packs_dir);
        std::filesystem::create_directories(packs_dir);
        const auto staging = repo / "staging" / job;
        std::filesystem::create_directories(staging);
        pack_id = random_id("pack-");
        temp = staging / (pack_id + ".tmp");
        published = pack_path(repo, pack_id);
        fd = ::open(temp.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (fd < 0) throw std::runtime_error(error_text("create staging object pack"));
        temp_exists = true;
        measured.setup_ns += elapsed_ns(setup_begin);

        try {
            std::array<std::uint8_t, kPackHeaderSize> pack_header{};
            std::copy(kPackMagic.begin(), kPackMagic.end(), pack_header.begin());
            put_le(pack_header.data() + 8, 1, 2);
            const auto write_begin = Clock::now();
            try {
                write_all(fd, pack_header.data(), pack_header.size());
            } catch (...) {
                measured.staging_write_ns += elapsed_ns(write_begin);
                throw;
            }
            measured.staging_write_ns += elapsed_ns(write_begin);
            measured.total_ns += elapsed_ns(operation_begin);
        } catch (...) {
            measured.total_ns += elapsed_ns(operation_begin);
            cleanup_temp();
            throw;
        }
    }

    ~BatchPackWriter() { cleanup_temp(); }

    void cleanup_temp() noexcept {
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
        if (temp_exists && !temp.empty()) {
            ::unlink(temp.c_str());
            temp_exists = false;
        }
    }

    WriteAck append(BlobWriteTask& task) {
        if (fd < 0 || published_successfully) throw std::runtime_error("cannot append to a closed object pack");
        if (objects >= 64) throw std::runtime_error("object pack reached its maximum object count");
        if (task.codec > 1 || task.raw_bytes > kChunkSize || task.stored.size() > ZSTD_compressBound(kChunkSize))
            throw std::runtime_error("object length or codec exceeds pack format limits");
        const std::uint64_t stored_bytes = static_cast<std::uint64_t>(task.stored.size());
        const auto max_offset = static_cast<std::uint64_t>(std::numeric_limits<off_t>::max());
        if (bytes > max_offset - kObjectHeaderSize ||
            stored_bytes > max_offset - bytes - kObjectHeaderSize)
            throw std::runtime_error("object pack exceeds platform file offset limits");

        WriteAck acknowledgement;
        acknowledgement.header = ObjectHeader{task.codec, task.raw_bytes, stored_bytes, task.digest, true, pack_id, bytes};
        acknowledgement.new_object = true;

        std::array<std::uint8_t, kObjectHeaderSize> header{};
        std::copy(kMagic.begin(), kMagic.end(), header.begin());
        put_le(header.data() + 8, 1, 2);
        header[10] = task.codec;
        put_le(header.data() + 16, task.raw_bytes, 8);
        put_le(header.data() + 24, stored_bytes, 8);
        std::copy(task.digest.begin(), task.digest.end(), header.begin() + 32);

        const auto operation_begin = Clock::now();
        const auto write_begin = Clock::now();
        try {
            write_all(fd, header.data(), header.size());
            if (!task.stored.empty()) write_all(fd, task.stored.data(), task.stored.size());
        } catch (...) {
            measured.staging_write_ns += elapsed_ns(write_begin);
            measured.total_ns += elapsed_ns(operation_begin);
            throw;
        }
        measured.staging_write_ns += elapsed_ns(write_begin);
        bytes += kObjectHeaderSize + stored_bytes;
        ++objects;
        measured.total_ns += elapsed_ns(operation_begin);
        return acknowledgement;
    }

    void publish() {
        if (fd < 0 || published_successfully) throw std::runtime_error("object pack is already closed");
        if (objects == 0) throw std::runtime_error("cannot publish an empty object pack");
        const auto operation_begin = Clock::now();
        try {
            maybe_failpoint(failpoint, "after-temp-write");

            auto phase_begin = Clock::now();
            const int sync_result = ::fsync(fd);
            const int sync_error = errno;
            measured.file_sync_ns += elapsed_ns(phase_begin);
            if (sync_result != 0) { errno = sync_error; throw std::runtime_error(error_text("sync staging object pack")); }

            phase_begin = Clock::now();
            const int close_result = ::close(fd);
            const int close_error = errno;
            measured.cleanup_ns += elapsed_ns(phase_begin);
            fd = -1;
            if (close_result != 0) { errno = close_error; throw std::runtime_error(error_text("close staging object pack")); }

            phase_begin = Clock::now();
            const int link_result = ::link(temp.c_str(), published.c_str());
            const int link_error = errno;
            measured.publish_ns += elapsed_ns(phase_begin);
            if (link_result != 0) { errno = link_error; throw std::runtime_error(error_text("publish object pack")); }
            maybe_failpoint(failpoint, "after-object-publish");

            phase_begin = Clock::now();
            const int unlink_result = ::unlink(temp.c_str());
            const int unlink_error = errno;
            measured.cleanup_ns += elapsed_ns(phase_begin);
            if (unlink_result != 0) { errno = unlink_error; throw std::runtime_error(error_text("remove published staging pack")); }
            temp_exists = false;

            phase_begin = Clock::now();
            fsync_directory(packs_dir);
            if (!packs_dir_existed) fsync_directory(objects_dir);
            measured.directory_sync_ns += elapsed_ns(phase_begin);
            published_successfully = true;
            measured.total_ns += elapsed_ns(operation_begin);
        } catch (...) {
            measured.total_ns += elapsed_ns(operation_begin);
            throw;
        }
    }
};

std::vector<WriteAck> write_pack(const std::filesystem::path& repo, const std::string& job,
                                 const std::vector<BlobWriteTask*>& tasks, const std::string& failpoint,
                                 ObjectWriteTimings* timings) {
    if (tasks.empty()) throw std::runtime_error("cannot publish an empty object pack");
    ObjectWriteTimings discarded;
    auto* measured = timings ? timings : &discarded;
    *measured = {};
    BatchPackWriter writer(repo, job, failpoint);
    try {
        std::vector<WriteAck> acknowledgements;
        acknowledgements.reserve(tasks.size());
        for (auto* task : tasks) {
            if (!task) throw std::runtime_error("object pack contains an empty task");
            acknowledgements.push_back(writer.append(*task));
        }
        writer.publish();
        *measured = writer.measured;
        return acknowledgements;
    } catch (...) {
        *measured = writer.measured;
        throw;
    }
}
} // namespace pbe::detail
