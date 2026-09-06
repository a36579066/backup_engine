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
#include <string>
#include <unordered_map>
#include <vector>
#include <exception>

namespace pbe::detail {
inline constexpr std::uint64_t kChunkSize = 1u << 20;
inline constexpr std::size_t kObjectHeaderSize = 64;

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
    std::uint64_t file_sync_ns = 0;
    std::uint64_t publish_ns = 0;
    std::uint64_t directory_sync_ns = 0;
    std::uint64_t cleanup_ns = 0;
    std::uint64_t existing_check_ns = 0;
    std::uint64_t total_ns = 0;
};

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

struct WriteAck { ObjectHeader header; bool new_object = false; };
struct BlobWriteTask {
    Digest digest{};
    std::uint64_t raw_bytes = 0;
    std::uint8_t codec = 0;
    std::vector<std::uint8_t> stored;
    std::shared_ptr<std::promise<WriteAck>> promise;
};

std::filesystem::path object_path(const std::filesystem::path& repo, const std::string& id);
ObjectHeader read_object_header(const std::filesystem::path& path);
std::vector<std::uint8_t> read_object(const std::filesystem::path& path, const std::string& expected_id,
                                      ObjectReadTimings* timings = nullptr);
WriteAck write_object(const std::filesystem::path& repo, const std::string& job,
                      BlobWriteTask& task, const std::string& failpoint,
                      ObjectWriteTimings* timings = nullptr);

void fsync_directory(const std::filesystem::path& path);
} // namespace pbe::detail
