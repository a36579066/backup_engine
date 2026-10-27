#pragma once

#include "storage/object_store.hpp"
#include "pipeline/shared/messages.hpp"

#include <atomic>
#include <cstdint>
#include <future>
#include <string>
#include <vector>

#include <sqlite3.h>

namespace pbe::detail {
struct MetadataCommit {
    std::uint64_t unique_raw_bytes = 0;
    std::uint64_t reused_chunks = 0;
    std::uint64_t ack_wait_ns = 0;
    std::uint64_t transaction_ns = 0;
    std::vector<std::string> object_ids;
};

// Wait for durable results before opening a transaction; never hold a write transaction over I/O waits.
// The dedicated Metadata Writer owns the connection and calls this function in FIFO batch order.
MetadataCommit commit_metadata_batch(sqlite3* db, const std::string& snapshot, MetadataBatch& batch,
                                     const std::string& failpoint,
                                     const std::atomic<bool>& cancelled);
} // namespace pbe::detail
