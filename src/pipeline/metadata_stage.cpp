#include "pipeline/metadata_stage.hpp"

#include "pbe/blake3.hpp"
#include "storage/catalog.hpp"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <stdexcept>

namespace pbe::detail {
namespace {
using Clock = std::chrono::steady_clock;
std::uint64_t elapsed_ns(Clock::time_point start) {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());
}
} // namespace

MetadataCommit commit_metadata_batch(sqlite3* db, const std::string& snapshot,
                                    MetadataBatch& batch, const std::string& failpoint,
                                    const std::atomic<bool>& cancelled) {
    MetadataCommit result;
    std::vector<ObjectHeader> headers;
    headers.reserve(batch.chunks.size());
    result.object_ids.reserve(batch.chunks.size());
    const auto wait_begin = Clock::now();
    for (auto& chunk : batch.chunks) {
        if (!chunk.result.valid()) throw std::runtime_error("metadata stage received an invalid chunk future");
        while (chunk.result.wait_for(std::chrono::milliseconds(10)) != std::future_status::ready) {
            if (cancelled.load(std::memory_order_relaxed))
                throw std::runtime_error("backup cancelled while waiting for durable chunk metadata");
        }
        if (cancelled.load(std::memory_order_relaxed)) throw std::runtime_error("backup cancelled");
        auto header = chunk.result.get();
        if (header.raw_bytes != chunk.bytes) throw std::runtime_error("durable chunk acknowledgement has an incorrect length");
        result.object_ids.push_back(digest_hex(header.digest));
        if (header.newly_written) result.unique_raw_bytes += header.raw_bytes;
        else ++result.reused_chunks;
        headers.push_back(std::move(header));
    }
    result.ack_wait_ns = elapsed_ns(wait_begin);
    if (batch.entries.empty() && batch.chunks.empty()) return result;
    if (cancelled.load(std::memory_order_relaxed)) throw std::runtime_error("backup cancelled");

    const auto transaction_begin = Clock::now();
    Transaction transaction(db);
    Statement entry(db, "INSERT INTO entries(snapshot_id,relative_path,kind,size_bytes,mode_bits,mtime_ns,symlink_target) VALUES(?,?,?,?,?,?,?)");
    for (const auto& value : batch.entries) {
        entry.text(1, snapshot); entry.text(2, value.path); entry.text(3, value.kind);
        entry.integer(4, static_cast<std::int64_t>(value.size)); entry.integer(5, value.mode); entry.integer(6, value.mtime_ns);
        if (value.kind == "symlink") entry.text(7, value.symlink_target); else entry.null(7);
        entry.done(); entry.reset();
    }
    const bool packed = !headers.empty() && !headers.front().pack_id.empty();
    Statement object(db, packed
        ? "INSERT INTO objects(object_id,raw_bytes,stored_bytes,codec,pack_id,pack_offset) VALUES(?,?,?,?,?,?) ON CONFLICT(object_id) DO UPDATE SET raw_bytes=excluded.raw_bytes,stored_bytes=excluded.stored_bytes,codec=excluded.codec,pack_id=excluded.pack_id,pack_offset=excluded.pack_offset"
        : "INSERT INTO objects(object_id,raw_bytes,stored_bytes,codec) VALUES(?,?,?,?) ON CONFLICT(object_id) DO UPDATE SET raw_bytes=excluded.raw_bytes,stored_bytes=excluded.stored_bytes,codec=excluded.codec");
    Statement reference(db, "INSERT INTO entry_chunks(snapshot_id,relative_path,ordinal,object_id,chunk_bytes) VALUES(?,?,?,?,?)");
    for (std::size_t i = 0; i < batch.chunks.size(); ++i) {
        const auto& chunk = batch.chunks[i];
        const auto& header = headers[i];
        const auto& id = result.object_ids[i];
        object.text(1, id); object.integer(2, static_cast<std::int64_t>(header.raw_bytes));
        object.integer(3, static_cast<std::int64_t>(header.stored_bytes)); object.text(4, header.codec ? "zstd" : "raw");
        if (packed) { object.text(5, header.pack_id); object.integer(6, static_cast<std::int64_t>(header.pack_offset)); }
        object.done(); object.reset();
        reference.text(1, snapshot); reference.text(2, chunk.path);
        reference.integer(3, static_cast<std::int64_t>(chunk.ordinal)); reference.text(4, id);
        reference.integer(5, static_cast<std::int64_t>(chunk.bytes));
        reference.done(); reference.reset();
    }
    if (failpoint == "before-sqlite-batch-commit") {
        std::cerr << "PBE fault injection: before-sqlite-batch-commit\n" << std::flush;
        std::_Exit(86);
    }
    transaction.commit();
    result.transaction_ns = elapsed_ns(transaction_begin);
    return result;
}
} // namespace pbe::detail
