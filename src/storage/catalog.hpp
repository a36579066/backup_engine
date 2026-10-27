#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

#include <sqlite3.h>

namespace pbe::detail {
// RAII prepared statement; finalize on destruction and reuse through reset/rebind.
class Statement {
public:
    Statement(sqlite3* db, const char* sql);
    ~Statement();
    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;
    void text(int index, const std::string& value);
    void integer(int index, std::int64_t value);
    void null(int index);
    bool row();
    void done();
    void reset();
    std::string string(int index) const;
    std::int64_t i64(int index) const;
    bool is_null(int index) const;

private:
    sqlite3* db_ = nullptr;
    sqlite3_stmt* stmt_ = nullptr;
};

void exec(sqlite3* db, const char* sql);

// Begin on construction; commit explicitly or roll back on scope exit.
class Transaction {
public:
    explicit Transaction(sqlite3* db);
    ~Transaction();
    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;
    void commit();

private:
    sqlite3* db_;
    bool done_ = false;
};

// Own one SQLite connection, used by its stage rather than shared across workers.
// Readers enforce query_only; FAT32 may need a writable handle for SQLite WAL maintenance.
class Catalog {
public:
    explicit Catalog(const std::filesystem::path& path, bool create = false);
    ~Catalog();
    Catalog(const Catalog&) = delete;
    Catalog& operator=(const Catalog&) = delete;
    sqlite3* get() const;

private:
    sqlite3* db_ = nullptr;
};

void create_schema(sqlite3* db);
} // namespace pbe::detail
