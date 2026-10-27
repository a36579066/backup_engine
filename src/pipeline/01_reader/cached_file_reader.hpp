#pragma once

#include <unordered_map>

#include "pipeline/01_reader/metadata_batcher.hpp"
#include "storage/catalog.hpp"

namespace pbe::detail {
struct CachedEntry {
    std::uint64_t size = 0;
    std::uint32_t mode = 0;
    std::int64_t mtime_ns = 0;
    std::string kind;
    std::string symlink_target;
};

// Trusted-metadata fast path reuses chunk references from a previous snapshot.
// Matching size, mode, and mtime cannot prove unchanged content; require explicit trust_metadata.
using MetadataCache = std::unordered_map<std::string, CachedEntry>;
MetadataCache load_metadata_cache(sqlite3* db, const std::string& source, std::string& snapshot_id);
bool can_reuse(const MetadataCache& cache, const SourceEntry& entry);
void reuse_cached_file(PipelineContext& context, MetadataBatcher& metadata, Catalog& source_catalog,
                       const std::string& previous_snapshot, const SourceEntry& entry);

} // namespace pbe::detail
