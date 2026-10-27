#pragma once

#include "concurrency/memory_budget.hpp"
#include "storage/object_store.hpp"
#include "storage/snapshot_catalog.hpp"

#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <vector>

namespace pbe::detail {
// Stage 01 -> 02: raw bytes, buffer credit, and a promise for the final object result.
// Move raw bytes and credit together so capacity is not returned while the buffer is still in use.
struct ChunkTask {
    std::uint64_t ordinal = 0;
    std::vector<std::uint8_t> raw;
    MemoryBudget::Credit credit;
    std::shared_ptr<std::promise<ObjectHeader>> promise;
};

// Stage 01 -> 04: file path, chunk ordinal, length, and a shared future for its object result.
// Metadata obtains ObjectHeader through this result; phased readers also keep a copy to await the batch.
struct PendingChunk {
    std::string path;
    std::uint64_t ordinal = 0;
    std::uint64_t bytes = 0;
    std::shared_future<ObjectHeader> result;
};

// Reader-accumulated file entries and chunk references, sent to the sole Metadata Writer.
struct MetadataBatch {
    std::vector<DbEntry> entries;
    std::vector<PendingChunk> chunks;
    std::uint64_t completed_files = 0;

    bool empty() const {
        return entries.empty() && chunks.empty() && completed_files == 0;
    }
};
} // namespace pbe::detail
