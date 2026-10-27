#include "pipeline/01_reader/cached_file_reader.hpp"
#include "pipeline/shared/pipeline_context.hpp"
#include "pipeline/shared/stage_support.hpp"

#include <algorithm>

namespace pbe::detail {
using Clock = PipelineClock;

std::unordered_map<std::string, CachedEntry>
load_metadata_cache(sqlite3* db, const std::string& source, std::string& snapshot_id) {
    Statement previous(
        db,
        "SELECT id FROM snapshots WHERE source_root=? AND status='COMPLETE' ORDER BY created_ns DESC LIMIT 1");
    previous.text(1, source);
    if (!previous.row())
        return {};
    snapshot_id = previous.string(0);
    Statement entries(
        db,
        "SELECT relative_path,kind,size_bytes,mode_bits,mtime_ns,COALESCE(symlink_target,'') FROM entries WHERE snapshot_id=?");
    entries.text(1, snapshot_id);
    std::unordered_map<std::string, CachedEntry> cache;
    while (entries.row())
        cache.emplace(entries.string(0),
                      CachedEntry{static_cast<std::uint64_t>(entries.i64(2)),
                                  static_cast<std::uint32_t>(entries.i64(3)), entries.i64(4),
                                  entries.string(1), entries.string(5)});
    return cache;
}

bool can_reuse(const MetadataCache& metadata_cache, const SourceEntry& entry) {
    const auto cache = metadata_cache.find(entry.relative);
    return cache != metadata_cache.end() && cache->second.kind == "file" &&
           cache->second.size == entry.size && cache->second.mode == entry.mode &&
           cache->second.mtime_ns == entry.mtime_ns;
}

void reuse_cached_file(PipelineContext& context, MetadataBatcher& metadata, Catalog& source_catalog,
                       const std::string& previous_snapshot, const SourceEntry& entry) {
    metadata.queue_entry(entry);

    // Load references in bounded batches instead of retaining an entire large file's chunk map.
    constexpr std::size_t cache_batch_size = 128;
    std::uint64_t next_ordinal = 0;
    std::uint64_t expected_ordinal = 0;
    std::uint64_t cached_bytes = 0;
    for (;;) {
        std::vector<ChunkRef> refs;
        {
            const char* query_sql =
                context.packed_storage
                    ? "SELECT c.ordinal,c.object_id,c.chunk_bytes,o.pack_id,o.pack_offset FROM entry_chunks c JOIN objects o ON o.object_id=c.object_id WHERE c.snapshot_id=? AND c.relative_path=? AND c.ordinal>=? ORDER BY c.ordinal LIMIT ?"
                    : "SELECT ordinal,object_id,chunk_bytes FROM entry_chunks WHERE snapshot_id=? AND relative_path=? AND ordinal>=? ORDER BY ordinal LIMIT ?";
            Statement query(source_catalog.get(), query_sql);
            query.text(1, previous_snapshot);
            query.text(2, entry.relative);
            query.integer(3, static_cast<std::int64_t>(next_ordinal));
            query.integer(4, cache_batch_size);
            while (query.row()) {
                ChunkRef ref{static_cast<std::uint64_t>(query.i64(0)),
                             static_cast<std::uint64_t>(query.i64(2)),
                             query.string(1),
                             {}};
                if (context.packed_storage) {
                    if (query.is_null(3) || query.is_null(4))
                        throw std::runtime_error(
                            "cached packed object is missing its catalog location");
                    ref.location =
                        ObjectLocation{query.string(3), static_cast<std::uint64_t>(query.i64(4))};
                }
                refs.push_back(std::move(ref));
            }
        }
        if (refs.empty())
            break;
        std::vector<ObjectHeader> headers;
        headers.reserve(refs.size());
        const std::uint64_t batch_start_bytes = cached_bytes;
        for (const auto& ref : refs) {
            const std::uint64_t expected_bytes =
                entry.size > cached_bytes
                    ? std::min<std::uint64_t>(kChunkSize, entry.size - cached_bytes)
                    : 0;
            if (ref.ordinal != expected_ordinal || expected_bytes == 0 ||
                ref.bytes != expected_bytes)
                throw std::runtime_error("cached chunk map is invalid for " + entry.relative);
            const auto header_begin = Clock::now();
            auto header = context.packed_storage
                              ? detail::read_packed_object_header(context.repo, ref.location)
                              : read_object_header(object_path(context.repo, ref.object_id));
            context.metrics.object_io_ns.fetch_add(elapsed_ns(header_begin),
                                                   std::memory_order_relaxed);
            if (header.raw_bytes != ref.bytes || digest_hex(header.digest) != ref.object_id)
                throw std::runtime_error("cached object metadata mismatch");
            const bool verify_object =
                !context.verified_existing_objects.contains_or_insert(ref.object_id);

            // Reused references still require content validation; the recent cache avoids repeat reads/decodes.
            if (verify_object) {
                if (!context.gate.acquire(context.errors.cancelled))
                    context.errors.rethrow();
                try {
                    auto credit =
                        context.memory.acquire(context.credit_per_chunk, context.errors.cancelled);
                    ObjectReadTimings timings;
                    const auto raw = context.packed_storage
                                         ? detail::read_packed_object(context.repo, ref.location,
                                                                      ref.object_id, &timings)
                                         : read_object(object_path(context.repo, ref.object_id),
                                                       ref.object_id, &timings);
                    record_object_read(context.metrics, timings);
                    if (raw.size() != ref.bytes)
                        throw std::runtime_error("cached object content length mismatch");
                } catch (...) {
                    context.gate.release();
                    throw;
                }
                context.gate.release();
            }
            headers.push_back(header);
            ++expected_ordinal;
            cached_bytes += ref.bytes;
        }

        // Route verified objects through the same Metadata path using already-ready futures.
        for (std::size_t i = 0; i < refs.size(); ++i) {
            std::promise<ObjectHeader> ready;
            auto result = ready.get_future();
            ready.set_value(std::move(headers[i]));
            metadata.queue_chunk(entry.relative, refs[i].ordinal, refs[i].bytes, std::move(result));
        }
        context.metrics.completed_chunks.fetch_add(refs.size(), std::memory_order_relaxed);
        context.metrics.completed_raw_bytes.fetch_add(cached_bytes - batch_start_bytes,
                                                      std::memory_order_relaxed);
        next_ordinal = refs.back().ordinal + 1;
    }
    if (cached_bytes != entry.size)
        throw std::runtime_error("cached chunk map is incomplete for " + entry.relative);
    metadata.complete_file();
}
} // namespace pbe::detail
