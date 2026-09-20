#include "storage/snapshot_catalog.hpp"

#include "storage/catalog.hpp"

#include <stdexcept>

namespace pbe::detail {
std::vector<std::string> path_parts(const std::string& relative) {
    if (relative.empty() || relative.front() == '/' || relative.find('\0') != std::string::npos)
        throw std::runtime_error("unsafe relative path in snapshot");
    std::vector<std::string> parts;
    std::size_t begin = 0;
    while (begin <= relative.size()) {
        const auto slash = relative.find('/', begin);
        const auto end = slash == std::string::npos ? relative.size() : slash;
        const auto part = relative.substr(begin, end - begin);
        if (part.empty() || part == "." || part == "..") throw std::runtime_error("unsafe relative path in snapshot: " + relative);
        parts.push_back(part);
        if (slash == std::string::npos) break;
        begin = slash + 1;
    }
    return parts;
}

std::vector<DbEntry> load_entries(sqlite3* db, const std::string& snapshot) {
    Statement statement(db, "SELECT relative_path,kind,size_bytes,mode_bits,mtime_ns,COALESCE(symlink_target,'') FROM entries WHERE snapshot_id=? ORDER BY relative_path");
    statement.text(1, snapshot);
    std::vector<DbEntry> entries;
    while (statement.row()) entries.push_back(DbEntry{statement.string(0), statement.string(1), statement.string(5),
        static_cast<std::uint64_t>(statement.i64(2)), static_cast<std::uint32_t>(statement.i64(3)), statement.i64(4)});
    return entries;
}

std::string require_complete(sqlite3* db, const std::string& snapshot) {
    Statement statement(db, "SELECT status FROM snapshots WHERE id=?");
    statement.text(1, snapshot);
    if (!statement.row()) throw std::runtime_error("snapshot not found: " + snapshot);
    if (statement.string(0) != "COMPLETE") throw std::runtime_error("snapshot is not complete: " + snapshot);
    return snapshot;
}

SnapshotSummary query_snapshot(sqlite3* db, const std::string& snapshot) {
    Statement statement(db,
        "SELECT s.id,s.created_ns,s.source_root,"
        "SUM(CASE WHEN e.kind='file' THEN 1 ELSE 0 END),"
        "SUM(CASE WHEN e.kind='directory' THEN 1 ELSE 0 END),"
        "SUM(CASE WHEN e.kind='symlink' THEN 1 ELSE 0 END),"
        "(SELECT COUNT(*) FROM entry_chunks c WHERE c.snapshot_id=s.id),"
        "COALESCE(SUM(CASE WHEN e.kind='file' THEN e.size_bytes ELSE 0 END),0) "
        "FROM snapshots s LEFT JOIN entries e ON e.snapshot_id=s.id "
        "WHERE s.id=? AND s.status='COMPLETE' GROUP BY s.id");
    statement.text(1, snapshot);
    if (!statement.row()) throw std::runtime_error("complete snapshot not found: " + snapshot);
    return SnapshotSummary{statement.string(0), statement.i64(1), statement.string(2),
        static_cast<std::uint64_t>(statement.i64(3)), static_cast<std::uint64_t>(statement.i64(4)),
        static_cast<std::uint64_t>(statement.i64(5)), static_cast<std::uint64_t>(statement.i64(6)),
        static_cast<std::uint64_t>(statement.i64(7))};
}

std::vector<ChunkRef> load_chunks(sqlite3* db, const std::string& snapshot, const std::string& path,
                                  bool packed_storage) {
    Statement statement(db, packed_storage
        ? "SELECT c.ordinal,c.object_id,c.chunk_bytes,o.pack_id,o.pack_offset FROM entry_chunks c JOIN objects o ON o.object_id=c.object_id WHERE c.snapshot_id=? AND c.relative_path=? ORDER BY c.ordinal"
        : "SELECT ordinal,object_id,chunk_bytes FROM entry_chunks WHERE snapshot_id=? AND relative_path=? ORDER BY ordinal");
    statement.text(1, snapshot); statement.text(2, path);
    std::vector<ChunkRef> chunks;
    while (statement.row()) {
        ChunkRef ref{static_cast<std::uint64_t>(statement.i64(0)), static_cast<std::uint64_t>(statement.i64(2)), statement.string(1), {}};
        if (packed_storage) {
            if (statement.is_null(3) || statement.is_null(4)) throw std::runtime_error("packed object is missing its catalog location");
            ref.location = ObjectLocation{statement.string(3), static_cast<std::uint64_t>(statement.i64(4))};
        }
        chunks.push_back(std::move(ref));
    }
    return chunks;
}

std::optional<ObjectLocation> lookup_object_location(sqlite3* db, const std::string& object_id) {
    Statement statement(db, "SELECT pack_id,pack_offset FROM objects WHERE object_id=?");
    statement.text(1, object_id);
    if (!statement.row()) return std::nullopt;
    if (statement.is_null(0) || statement.is_null(1))
        throw std::runtime_error("object catalog row is missing its pack location: " + object_id);
    return ObjectLocation{statement.string(0), static_cast<std::uint64_t>(statement.i64(1))};
}

void ensure_chunk_layout(sqlite3* db, const std::string& snapshot) {
    Statement statement(db,
        "SELECT e.relative_path,e.size_bytes,COUNT(c.ordinal),COALESCE(SUM(c.chunk_bytes),0),"
        "COALESCE(MIN(c.ordinal),0),COALESCE(MAX(c.ordinal),-1) "
        "FROM entries e LEFT JOIN entry_chunks c ON e.snapshot_id=c.snapshot_id AND e.relative_path=c.relative_path "
        "WHERE e.snapshot_id=? AND e.kind='file' GROUP BY e.relative_path");
    statement.text(1, snapshot);
    while (statement.row()) {
        const auto size = static_cast<std::uint64_t>(statement.i64(1));
        const auto count = static_cast<std::uint64_t>(statement.i64(2));
        const auto sum = static_cast<std::uint64_t>(statement.i64(3));
        const auto min = statement.i64(4), max = statement.i64(5);
        if (sum != size || (count == 0 ? size != 0 : (min != 0 || static_cast<std::uint64_t>(max + 1) != count)))
            throw std::runtime_error("snapshot has an incomplete chunk map for " + statement.string(0));
    }
}
} // namespace pbe::detail
