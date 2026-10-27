#pragma once

#include "pbe/blake3.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <future>
#include <list>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>
#include <exception>

namespace pbe::detail {
inline constexpr std::uint64_t kChunkSize = 1u << 20;
inline constexpr std::size_t kObjectHeaderSize = 64;
inline constexpr std::uint64_t kPackTargetBytes = 128ull << 20;
inline constexpr std::size_t kPackMaxObjects = 256;
inline constexpr std::size_t kPackWriteBatchObjects = 8;

// Object codec, lengths, and digest; pack fields identify the stored representation's location.
struct ObjectHeader {
    std::uint8_t codec = 0;
    std::uint64_t raw_bytes = 0;
    std::uint64_t stored_bytes = 0;
    Digest digest{};
    bool newly_written = false;
    std::string pack_id;
    std::uint64_t pack_offset = 0;
};

struct ObjectLocation {
    std::string pack_id;
    std::uint64_t pack_offset = 0;
};

struct ObjectReadTimings {
    std::uint64_t io_ns = 0;
    std::uint64_t decompression_ns = 0;
    std::uint64_t hash_ns = 0;
    std::uint64_t total_ns = 0;
};

struct ObjectWriteTimings {
    std::uint64_t setup_ns = 0;
    std::uint64_t staging_write_ns = 0;
    std::uint64_t staging_write_calls = 0;
    std::uint64_t file_sync_ns = 0;
    std::uint64_t publish_ns = 0;
    std::uint64_t directory_sync_ns = 0;
    std::uint64_t cleanup_ns = 0;
    std::uint64_t existing_check_ns = 0;
    std::uint64_t total_ns = 0;
};

// Bounded recent verification cache; retains digests rather than decompressed payloads.
class VerifiedObjectCache {
public:
    explicit VerifiedObjectCache(std::size_t capacity = 4096);
    bool contains_or_insert(const std::string& object_id);
    std::size_t size() const;

private:
    const std::size_t capacity_;
    mutable std::mutex mutex_;
    std::list<std::string> recent_;
    std::unordered_map<std::string, std::list<std::string>::iterator> positions_;
};

struct WriteAck {
    ObjectHeader header;
    bool new_object = false;
};

// CPU -> Storage task: stored holds the payload and completion reports the final result.
// Type-erased credit ownership keeps Storage independent of the MemoryBudget type.
struct BlobWriteTask {
    Digest digest{};
    std::uint64_t raw_bytes = 0;
    std::uint8_t codec = 0;
    std::vector<std::uint8_t> stored;
    std::shared_ptr<std::promise<WriteAck>> promise;
    std::function<void(const WriteAck*, std::exception_ptr)> completion;
    std::shared_ptr<void> retained_credit;
};

std::filesystem::path object_path(const std::filesystem::path& repo, const std::string& id);
ObjectHeader read_object_header(const std::filesystem::path& path);
std::vector<std::uint8_t> read_object(const std::filesystem::path& path,
                                      const std::string& expected_id,
                                      ObjectReadTimings* timings = nullptr);
ObjectHeader read_packed_object_header(const std::filesystem::path& repo,
                                       const ObjectLocation& location);
std::vector<std::uint8_t> read_packed_object(const std::filesystem::path& repo,
                                             const ObjectLocation& location,
                                             const std::string& expected_id,
                                             ObjectReadTimings* timings = nullptr);
WriteAck write_object(const std::filesystem::path& repo, const std::string& job,
                      BlobWriteTask& task, const std::string& failpoint,
                      ObjectWriteTimings* timings = nullptr);

// Stage multiple objects in one temporary pack, then sync and publish them together.
// append results describe staged objects; report success only after publish() completes.
class PackWriter {
public:
    PackWriter(const std::filesystem::path& repo, const std::string& job,
               const std::string& failpoint);
    ~PackWriter();
    PackWriter(PackWriter&&) noexcept;
    PackWriter& operator=(PackWriter&&) noexcept;
    PackWriter(const PackWriter&) = delete;
    PackWriter& operator=(const PackWriter&) = delete;

    WriteAck append(BlobWriteTask& task);
    std::vector<WriteAck> append_batch(std::span<BlobWriteTask*> tasks);
    std::uint64_t bytes_written() const noexcept;
    std::size_t object_count() const noexcept;
    void publish();
    const ObjectWriteTimings& timings() const noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

void fsync_directory(const std::filesystem::path& path);
} // namespace pbe::detail
