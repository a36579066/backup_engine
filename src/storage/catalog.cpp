#include "storage/catalog.hpp"

#include <stdexcept>

namespace pbe::detail {
Statement::Statement(sqlite3* db, const char* sql) : db_(db) {
    if (sqlite3_prepare_v2(db, sql, -1, &stmt_, nullptr) != SQLITE_OK) throw std::runtime_error(sqlite3_errmsg(db));
}

Statement::~Statement() { sqlite3_finalize(stmt_); }

void Statement::text(int index, const std::string& value) {
    if (sqlite3_bind_text(stmt_, index, value.data(), static_cast<int>(value.size()), SQLITE_TRANSIENT) != SQLITE_OK)
        throw std::runtime_error(sqlite3_errmsg(db_));
}

void Statement::integer(int index, std::int64_t value) {
    if (sqlite3_bind_int64(stmt_, index, value) != SQLITE_OK) throw std::runtime_error(sqlite3_errmsg(db_));
}

void Statement::null(int index) {
    if (sqlite3_bind_null(stmt_, index) != SQLITE_OK) throw std::runtime_error(sqlite3_errmsg(db_));
}

bool Statement::row() {
    const int result = sqlite3_step(stmt_);
    if (result == SQLITE_ROW) return true;
    if (result == SQLITE_DONE) return false;
    throw std::runtime_error(sqlite3_errmsg(db_));
}

void Statement::done() {
    if (sqlite3_step(stmt_) != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(db_));
}

void Statement::reset() {
    if (sqlite3_reset(stmt_) != SQLITE_OK || sqlite3_clear_bindings(stmt_) != SQLITE_OK)
        throw std::runtime_error(sqlite3_errmsg(db_));
}

std::string Statement::string(int index) const {
    const auto* value = sqlite3_column_text(stmt_, index);
    if (!value) return {};
    const int bytes = sqlite3_column_bytes(stmt_, index);
    return std::string(reinterpret_cast<const char*>(value), static_cast<std::size_t>(bytes));
}

std::int64_t Statement::i64(int index) const { return sqlite3_column_int64(stmt_, index); }
bool Statement::is_null(int index) const { return sqlite3_column_type(stmt_, index) == SQLITE_NULL; }

void exec(sqlite3* db, const char* sql) {
    char* message = nullptr;
    if (sqlite3_exec(db, sql, nullptr, nullptr, &message) != SQLITE_OK) {
        std::string error = message ? message : sqlite3_errmsg(db);
        sqlite3_free(message);
        throw std::runtime_error(error);
    }
}

Transaction::Transaction(sqlite3* db) : db_(db) { exec(db_, "BEGIN IMMEDIATE"); }
Transaction::~Transaction() { if (!done_) sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr); }
void Transaction::commit() { exec(db_, "COMMIT"); done_ = true; }

Catalog::Catalog(const std::filesystem::path& path, bool create) {
    int flags = SQLITE_OPEN_FULLMUTEX | (create ? SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE : SQLITE_OPEN_READONLY);
    if (sqlite3_open_v2(path.c_str(), &db_, flags, nullptr) != SQLITE_OK) {
        std::string message = db_ ? sqlite3_errmsg(db_) : "sqlite open failed";
        if (db_) sqlite3_close(db_);
        db_ = nullptr;
        throw std::runtime_error(message);
    }
    sqlite3_busy_timeout(db_, 5000);
    if (create) exec(db_, "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;");
}

Catalog::~Catalog() { if (db_) sqlite3_close(db_); }
sqlite3* Catalog::get() const { return db_; }

void create_schema(sqlite3* db) {
    exec(db,
        "CREATE TABLE IF NOT EXISTS snapshots ("
        "id TEXT PRIMARY KEY, created_ns INTEGER NOT NULL, source_root TEXT NOT NULL,"
        "status TEXT NOT NULL CHECK(status IN ('IN_PROGRESS','COMPLETE','FAILED')),"
        "chunk_bytes INTEGER NOT NULL, completed_ns INTEGER);"
        "CREATE TABLE IF NOT EXISTS entries ("
        "snapshot_id TEXT NOT NULL, relative_path TEXT NOT NULL,"
        "kind TEXT NOT NULL CHECK(kind IN ('file','directory','symlink')),"
        "size_bytes INTEGER NOT NULL, mode_bits INTEGER NOT NULL, mtime_ns INTEGER NOT NULL,"
        "symlink_target TEXT, PRIMARY KEY(snapshot_id,relative_path),"
        "FOREIGN KEY(snapshot_id) REFERENCES snapshots(id));"
        "CREATE TABLE IF NOT EXISTS objects ("
        "object_id TEXT PRIMARY KEY, raw_bytes INTEGER NOT NULL, stored_bytes INTEGER NOT NULL,"
        "codec TEXT NOT NULL CHECK(codec IN ('raw','zstd')),pack_id TEXT,pack_offset INTEGER);"
        "CREATE TABLE IF NOT EXISTS entry_chunks ("
        "snapshot_id TEXT NOT NULL, relative_path TEXT NOT NULL, ordinal INTEGER NOT NULL,"
        "object_id TEXT NOT NULL, chunk_bytes INTEGER NOT NULL,"
        "PRIMARY KEY(snapshot_id,relative_path,ordinal),"
        "FOREIGN KEY(snapshot_id,relative_path) REFERENCES entries(snapshot_id,relative_path),"
        "FOREIGN KEY(object_id) REFERENCES objects(object_id));"
        "CREATE INDEX IF NOT EXISTS idx_snapshots_status_created ON snapshots(status,created_ns DESC);"
        "CREATE INDEX IF NOT EXISTS idx_chunks_object ON entry_chunks(object_id);");
}
} // namespace pbe::detail
