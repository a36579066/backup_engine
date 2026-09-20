#pragma once

#include "pbe/engine.hpp"
#include "storage/object_store.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <sqlite3.h>

namespace pbe::detail {
struct DbEntry {
    std::string path, kind, symlink_target;
    std::uint64_t size = 0;
    std::uint32_t mode = 0;
    std::int64_t mtime_ns = 0;
};

struct ChunkRef {
    std::uint64_t ordinal = 0, bytes = 0;
    std::string object_id;
    ObjectLocation location;
};

std::vector<std::string> path_parts(const std::string& relative);
std::vector<DbEntry> load_entries(sqlite3* db, const std::string& snapshot);
std::string require_complete(sqlite3* db, const std::string& snapshot);
SnapshotSummary query_snapshot(sqlite3* db, const std::string& snapshot);
std::vector<ChunkRef> load_chunks(sqlite3* db, const std::string& snapshot, const std::string& path,
                                  bool packed_storage = false);
std::optional<ObjectLocation> lookup_object_location(sqlite3* db, const std::string& object_id);
void ensure_chunk_layout(sqlite3* db, const std::string& snapshot);
} // namespace pbe::detail
