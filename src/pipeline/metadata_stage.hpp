#pragma once

#include "storage/object_store.hpp"
#include "storage/snapshot_catalog.hpp"

#include <atomic>
#include <cstdint>
#include <future>
#include <string>
#include <vector>

#include <sqlite3.h>

namespace pbe::detail {
struct PendingChunk {
    std::string path;
    std::uint64_t ordinal = 0;
    std::uint64_t bytes = 0;
    std::future<ObjectHeader> result;
};

// Only small metadata and futures cross this stage; chunk buffers stay in the
// reader -> CPU -> storage pipeline under MemoryBudget ownership.
struct MetadataBatch {
    std::vector<DbEntry> entries;
    std::vector<PendingChunk> chunks;
    std::uint64_t completed_files = 0;
    bool empty() const { return entries.empty() && chunks.empty() && completed_files == 0; }
};

struct MetadataCommit {
    std::uint64_t unique_raw_bytes = 0;
    std::uint64_t reused_chunks = 0;
    std::uint64_t ack_wait_ns = 0;
    std::uint64_t transaction_ns = 0;
    std::vector<std::string> object_ids;
};

// Wait for durable object acknowledgements BEFORE opening the transaction.
// One dedicated consumer owns the write connection and calls this in FIFO order.
MetadataCommit commit_metadata_batch(sqlite3* db, const std::string& snapshot,
                                    MetadataBatch& batch, const std::string& failpoint,
                                    const std::atomic<bool>& cancelled);
} // namespace pbe::detail
