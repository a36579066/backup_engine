#include "pbe/engine.hpp"
#include "scheduler/adaptive_controller.hpp"
#include "concurrency/claim_registry.hpp"
#include "concurrency/memory_budget.hpp"
#include "concurrency/worker_pool.hpp"
#include "scheduler/active_worker_gate.hpp"
#include "diagnostics/diagnostics.hpp"
#include "storage/object_store.hpp"
#include "pbe/blake3.hpp"
#include "pbe/bounded_queue.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <iostream>
#include <string>
#include <random>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>
#include <sqlite3.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace {
void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

void interrupt_handler(int) { pbe::note_interrupt(); }

void write_bytes(const fs::path& path, const std::vector<std::uint8_t>& bytes) {
    fs::create_directories(path.parent_path());
    std::ofstream out(path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    if (!out) throw std::runtime_error("fixture write failed");
}

std::vector<std::uint8_t> read_bytes(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    return std::vector<std::uint8_t>(std::istreambuf_iterator<char>(in), {});
}

std::string read_text(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), {});
}

bool same_tree(const fs::path& left, const fs::path& right) {
    std::vector<std::string> left_paths, right_paths;
    for (const auto& item : fs::recursive_directory_iterator(left)) left_paths.push_back(item.path().lexically_relative(left).generic_string());
    for (const auto& item : fs::recursive_directory_iterator(right)) right_paths.push_back(item.path().lexically_relative(right).generic_string());
    std::sort(left_paths.begin(), left_paths.end());
    std::sort(right_paths.begin(), right_paths.end());
    if (left_paths != right_paths) return false;
    for (const auto& relative : left_paths) {
        const auto a = left / fs::path(relative), b = right / fs::path(relative);
        const auto a_status = fs::symlink_status(a), b_status = fs::symlink_status(b);
        if (a_status.type() != b_status.type()) return false;
        if (fs::is_symlink(a_status)) {
            if (fs::read_symlink(a) != fs::read_symlink(b)) return false;
        } else if (fs::is_regular_file(a_status) && read_bytes(a) != read_bytes(b)) return false;
    }
    return true;
}

void update_catalog_text(const fs::path& repo, const char* sql, const std::string& value,
                         const std::string& snapshot) {
    sqlite3* db = nullptr;
    check(sqlite3_open_v2((repo / "catalog.sqlite").c_str(), &db, SQLITE_OPEN_READWRITE, nullptr) == SQLITE_OK,
          "open catalog to inject malformed metadata");
    sqlite3_stmt* statement = nullptr;
    check(sqlite3_prepare_v2(db, sql, -1, &statement, nullptr) == SQLITE_OK, "prepare catalog mutation");
    sqlite3_bind_text(statement, 1, value.data(), static_cast<int>(value.size()), SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, snapshot.data(), static_cast<int>(snapshot.size()), SQLITE_TRANSIENT);
    const int step = sqlite3_step(statement);
    const int changed = sqlite3_changes(db);
    sqlite3_finalize(statement);
    sqlite3_close(db);
    check(step == SQLITE_DONE && changed == 1, "catalog metadata mutation updated one row");
}

std::uint64_t read_le(const std::vector<std::uint8_t>& bytes, std::size_t offset, std::size_t count) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < count; ++i) value |= std::uint64_t(bytes[offset + i]) << (i * 8);
    return value;
}

void hash_vectors() {
    check(pbe::digest_hex(pbe::blake3({})) == "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262", "BLAKE3 empty vector");
    const std::string abc = "abc";
    check(pbe::digest_hex(pbe::blake3(std::span(reinterpret_cast<const std::uint8_t*>(abc.data()), abc.size()))) ==
          "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85", "BLAKE3 abc vector");
    check(pbe::digest_from_hex(pbe::digest_hex(pbe::blake3({}))) == pbe::blake3({}), "BLAKE3 hex conversion");
}

void queue_contract() {
    pbe::BoundedQueue<int> queue(1);
    check(queue.push(7), "queue accepts item");
    check(queue.size() == 1, "queue size");
    auto value = queue.pop();
    check(value && *value == 7, "queue returns item");
    queue.close();
    check(!queue.push(8), "closed queue rejects push");
    check(!queue.pop(), "closed and empty queue returns empty");
    pbe::BoundedQueue<int> full(1);
    check(full.push(1), "fill bounded queue");
    std::atomic<bool> cancelled{false};
    bool accepted = true;
    std::thread producer([&] { accepted = full.push(2, &cancelled); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    full.close();
    producer.join();
    check(!accepted, "blocked producer wakes when queue closes");

    pbe::BoundedQueue<int> cancelled_full(1);
    check(cancelled_full.push(1), "fill queue for cancellation test");
    std::atomic<bool> cancel_push{false};
    bool cancelled_push_accepted = true;
    std::thread blocked_push([&] { cancelled_push_accepted = cancelled_full.push(2, &cancel_push); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    cancel_push.store(true);
    blocked_push.join();
    check(!cancelled_push_accepted, "cancelled full-queue producer wakes and exits");

    pbe::BoundedQueue<int> cancelled_empty(1);
    std::atomic<bool> cancel_pop{false};
    std::optional<int> popped;
    std::thread blocked_pop([&] { popped = cancelled_empty.pop(&cancel_pop); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    cancel_pop.store(true);
    blocked_pop.join();
    check(!popped, "cancelled empty-queue consumer wakes and exits");
}

void concurrency_contract() {
    pbe::detail::MemoryBudget memory(128);
    std::atomic<bool> cancelled{false};
    {
        auto credit = memory.acquire(96, cancelled);
        check(memory.current() == 96 && memory.peak() == 96, "memory credits track current and peak usage");
    }
    check(memory.current() == 0, "memory credit releases through RAII");

    pbe::detail::FixedWorkerPool workers;
    std::atomic<std::size_t> started{0};
    workers.start(4, [&](std::size_t index) { if (index < 4) started.fetch_add(1); });
    workers.join();
    workers.join();
    check(workers.size() == 4 && started.load() == 4, "fixed worker pool starts and joins every worker");

    pbe::detail::ClaimRegistry<std::string> claims;
    auto [owner_state, owner] = claims.claim("digest");
    auto [waiter_state, duplicate_owner] = claims.claim("digest");
    check(owner && !duplicate_owner && owner_state == waiter_state, "duplicate digest shares an in-flight claim");
    claims.complete("digest", owner_state, "object-header");
    check(claims.size() == 1, "completed claim stays visible until metadata commit");
    check(claims.wait(waiter_state, cancelled) == "object-header", "existing waiters retain the completed state");

    auto [same_state, duplicate_after_complete] = claims.claim("digest");
    check(!duplicate_after_complete && same_state == owner_state &&
          claims.wait(same_state, cancelled) == "object-header",
          "duplicates arriving before metadata commit reuse the completed object");
    claims.release("digest");
    auto [next_state, next_owner] = claims.claim("digest");
    check(next_owner && next_state != owner_state && claims.size() == 1,
          "claim after committed metadata creates a new in-flight state");
    claims.fail("digest", next_state, std::make_exception_ptr(std::runtime_error("injected claim failure")));
    bool failed_as_expected = false;
    try { (void)claims.wait(next_state, cancelled); }
    catch (const std::runtime_error&) { failed_as_expected = true; }
    check(failed_as_expected && claims.size() == 0, "failed claim wakes waiters and is removed");
}

void diagnosis_rules() {
    pbe::detail::WindowSample sample;
    sample.elapsed_ns = 1'000'000'000;
    sample.completed_chunks = 8;
    sample.input_queue_fill = 0.8;
    sample.cpu_compute_ratio = 0.8;
    sample.writer_busy_ratio = 0.2;
    check(pbe::detail::classify(sample).classification == "CPU_LIMITED", "diagnostics identify compute pressure with input backlog");

    sample.cpu_compute_ratio = 0.1;
    sample.writer_busy_ratio = 0.95;
    sample.output_wait_ratio = 0.9;
    sample.writer_ack_wait_ratio = 0.9;
    check(pbe::detail::classify(sample).classification == "WRITER_LIMITED", "diagnostics identify writer backpressure");

    sample.memory_used_ratio = 0.9;
    sample.memory_wait_ratio = 0.2;
    check(pbe::detail::classify(sample).classification == "MEMORY_LIMITED", "diagnostics prioritize sustained memory pressure");

    sample.memory_used_ratio = 0;
    sample.memory_wait_ratio = 0;
    sample.writer_busy_ratio = 0;
    sample.output_wait_ratio = 0;
    sample.writer_ack_wait_ratio = 0;
    sample.input_queue_fill = 0;
    check(pbe::detail::classify(sample).classification == "INPUT_LIMITED", "diagnostics identify worker starvation");
}

void adaptive_controller_policy() {
    pbe::detail::AdaptiveController controller;
    pbe::detail::WindowSample sample;
    sample.completed_chunks = 8;
    sample.throughput_bytes_per_sec = 1000;
    const auto cpu = pbe::detail::Diagnosis{
        "CPU_LIMITED", "medium", "CPU_BACKLOG_R1", "CPU work is limiting throughput.", ""};

    auto decision = controller.evaluate(sample, cpu, 1, 4, true, false);
    check(decision.active_limit == 1 && decision.action == "HOLD" && decision.outcome == "WARMUP",
          "adaptive controller holds its first warmup window");
    decision = controller.evaluate(sample, cpu, 1, 4, true, false);
    check(decision.active_limit == 1 && decision.outcome == "WARMUP",
          "adaptive controller holds its second warmup window");
    decision = controller.evaluate(sample, cpu, 1, 4, true, false);
    check(decision.active_limit == 2 && decision.limit_changed && decision.action == "INCREASE_CPU_ADMISSION",
          "adaptive controller increases CPU admission by one permit when CPU limited");

    sample.throughput_bytes_per_sec = 700;
    decision = controller.evaluate(sample, cpu, 2, 4, true, false);
    check(decision.active_limit == 2 && decision.action == "HOLD",
          "adaptive controller holds while collecting the first regression comparison");
    decision = controller.evaluate(sample, cpu, 2, 4, true, false);
    check(decision.active_limit == 1 && decision.limit_changed &&
          decision.action == "ROLLBACK_CPU_ADMISSION" && decision.outcome == "ROLLED_BACK_BEST_EFFORT",
          "adaptive controller rolls back after two comparable regression windows");

    pbe::detail::AdaptiveController fixed_controller;
    decision = fixed_controller.evaluate(sample, cpu, 2, 4, false, false);
    check(decision.active_limit == 2 && !decision.limit_changed && decision.action == "HOLD",
          "fixed scheduler policy never changes the admission limit");
}

void object_store_contract() {
    pbe::detail::VerifiedObjectCache verified(2);
    check(!verified.contains_or_insert("a") && !verified.contains_or_insert("b"), "verification cache records new object IDs");
    check(verified.contains_or_insert("a"), "verification cache recognizes and refreshes a recent object");
    check(!verified.contains_or_insert("c") && verified.size() == 2, "verification cache evicts the least-recent ID at its bound");
    check(!verified.contains_or_insert("b") && verified.size() == 2, "evicted object IDs can be checked again without growing the cache");
    pbe::detail::VerifiedObjectCache concurrent_verified(32);
    std::atomic<std::size_t> cache_misses{0};
    std::vector<std::thread> cache_readers;
    for (std::size_t i = 0; i < 8; ++i) {
        cache_readers.emplace_back([&] {
            if (!concurrent_verified.contains_or_insert("shared-object")) cache_misses.fetch_add(1);
        });
    }
    for (auto& reader : cache_readers) reader.join();
    check(cache_misses.load() == 1, "concurrent object-cache lookups admit only one first verification");

    const auto repo = fs::temp_directory_path() /
        ("pbe-object-store-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::create_directories(repo / "objects");
    fs::create_directories(repo / "staging");
    const std::vector<std::uint8_t> raw(8192, 0x5a);
    pbe::detail::BlobWriteTask task;
    task.digest = pbe::blake3(raw);
    task.raw_bytes = raw.size();
    task.codec = 0;
    task.stored = raw;
    const auto id = pbe::digest_hex(task.digest);

    const auto first = pbe::detail::write_object(repo, "job", task, {});
    check(first.new_object && first.header.digest == task.digest, "object store publishes a new immutable object");
    pbe::detail::ObjectReadTimings timings;
    const auto loaded = pbe::detail::read_object(pbe::detail::object_path(repo, id), id, &timings);
    check(loaded == raw, "object store round-trips raw payload bytes");
    check(timings.io_ns > 0 && timings.hash_ns > 0 && timings.total_ns > 0,
          "object store reports read, hash, and verification timing");

    pbe::detail::BlobWriteTask duplicate = task;
    const auto second = pbe::detail::write_object(repo, "job", duplicate, {});
    check(!second.new_object && second.header.digest == task.digest,
          "object store preserves the existing object on a duplicate publish");
    fs::remove_all(repo);
}

void backup_round_trip() {
    const auto root = fs::temp_directory_path() / ("pbe-test-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const auto repo = root / "repo";
    const auto source = root / "source";
    const auto restored = root / "restored";
    fs::create_directories(source / "folder");
    std::vector<std::uint8_t> repeated(1u << 20, 'R');
    write_bytes(source / "folder" / "alpha.txt", repeated);
    write_bytes(source / "folder" / "beta.txt", repeated);
    std::vector<std::uint8_t> repeated_chunks;
    for (int i = 0; i < 8; ++i) repeated_chunks.insert(repeated_chunks.end(), repeated.begin(), repeated.end());
    write_bytes(source / "folder" / "repeated-eight.bin", repeated_chunks);
    write_bytes(source / "folder" / "tail.bin", std::vector<std::uint8_t>{1, 2, 3, 4, 5});
    write_bytes(source / ".hidden", std::vector<std::uint8_t>{});
    write_bytes(source / "資料" / "測試.txt", std::vector<std::uint8_t>{0, 1, 2, 0xfe, 0xff});
    fs::create_directories(source / "empty-dir");
    fs::create_symlink("folder/alpha.txt", source / "link-to-alpha");
    fs::permissions(source / "folder" / "alpha.txt", fs::perms::owner_read | fs::perms::owner_write | fs::perms::group_read,
                    fs::perm_options::replace);
    fs::last_write_time(source / "folder" / "alpha.txt", fs::file_time_type::clock::now() - std::chrono::hours(48));
    fs::permissions(source / "folder", fs::perms::owner_all | fs::perms::group_read | fs::perms::group_exec,
                    fs::perm_options::replace);
    fs::last_write_time(source / "folder", fs::file_time_type::clock::now() - std::chrono::hours(72));
    const auto alpha_permissions = fs::status(source / "folder" / "alpha.txt").permissions() & fs::perms::all;
    const auto alpha_time = fs::last_write_time(source / "folder" / "alpha.txt");
    const auto folder_permissions = fs::status(source / "folder").permissions() & fs::perms::all;
    const auto folder_time = fs::last_write_time(source / "folder");

    pbe::init_repository(repo);
    pbe::BackupOptions options;
    options.repo = repo; options.source = source; options.workers = 1; options.memory_mib = 16;
    const auto first = pbe::backup(options);
    check(first.memory_credit_peak_bytes <= options.memory_mib * 1024u * 1024u, "memory credits stay within budget");
    check(first.files == 6, "backup file count including Unicode path");
    check(first.directories == 3, "backup directory count including Unicode parent directory");
    check(first.symlinks == 1, "backup symlink count");
    check(first.reused_chunks >= 1, "duplicate chunk reuse");
    const auto verification = pbe::verify_snapshot(repo, first.id);
    check(verification.bytes == first.logical_bytes, "verify logical bytes");
    pbe::restore_snapshot({repo, restored, first.id});
    check(read_bytes(source / "folder" / "alpha.txt") == read_bytes(restored / "folder" / "alpha.txt"), "restore alpha");
    check(read_bytes(source / "folder" / "beta.txt") == read_bytes(restored / "folder" / "beta.txt"), "restore beta");
    check(read_bytes(source / "folder" / "repeated-eight.bin") == read_bytes(restored / "folder" / "repeated-eight.bin"), "restore repeated chunks");
    check(read_bytes(source / "folder" / "tail.bin") == read_bytes(restored / "folder" / "tail.bin"), "restore tail");
    check(fs::is_empty(restored / ".hidden"), "restore empty hidden file");
    check(read_bytes(source / "資料" / "測試.txt") == read_bytes(restored / "資料" / "測試.txt"), "restore Unicode path and bytes");
    check(fs::is_symlink(restored / "link-to-alpha"), "restore symlink metadata");
    check(same_tree(source, restored), "full restored directory tree matches source names and bytes");
    check((fs::status(restored / "folder" / "alpha.txt").permissions() & fs::perms::all) == alpha_permissions,
          "restore preserves regular-file mode bits");
    check(fs::last_write_time(restored / "folder" / "alpha.txt") == alpha_time, "restore preserves regular-file mtime");
    check((fs::status(restored / "folder").permissions() & fs::perms::all) == folder_permissions,
          "restore preserves directory mode bits");
    check(fs::last_write_time(restored / "folder") == folder_time, "restore preserves directory mtime");
    const auto trace_path = repo / "reports" / (first.run_id + ".jsonl");
    const auto trace = read_text(trace_path);
    check(trace.find("\"schema_version\":5") != std::string::npos &&
          trace.find("\"active_limit_before\"") != std::string::npos &&
          trace.find("\"writer_workers\":1") != std::string::npos &&
          trace.find("\"hash_ms\"") != std::string::npos &&
          trace.find("\"compression_ms\"") != std::string::npos &&
          trace.find("\"object_verify_ms\"") != std::string::npos &&
          trace.find("\"object_io_ms\"") != std::string::npos &&
          trace.find("\"source_read_ms\"") != std::string::npos &&
          trace.find("\"catalog_commit_ms\"") != std::string::npos &&
          trace.find("\"object_file_sync_ms\"") != std::string::npos &&
          trace.find("\"object_directory_sync_ms\"") != std::string::npos &&
          trace.find("\"object_write_count\"") != std::string::npos &&
          trace.find("\"object_write_operation_count\"") != std::string::npos &&
          trace.find("\"rule_id\"") != std::string::npos && trace.find("\"outcome\"") != std::string::npos,
          "decision trace has versioned observations and outcomes");
    bool overwrite_rejected = false;
    try { pbe::restore_snapshot({repo, restored, first.id}); } catch (...) { overwrite_rejected = true; }
    check(overwrite_rejected, "restore refuses a non-empty target");
    const auto occupied = root / "occupied";
    fs::create_directories(occupied); write_bytes(occupied / "keep.txt", std::vector<std::uint8_t>{1});
    overwrite_rejected = false;
    try { pbe::restore_snapshot({repo, occupied, first.id}); } catch (...) { overwrite_rejected = true; }
    check(overwrite_rejected && fs::exists(occupied / "keep.txt"), "restore preserves pre-existing target files");

    for (const std::size_t workers : {2u, 4u, 8u}) {
        options.workers = workers;
        options.adaptive = workers == 8;
        const auto parallel = pbe::backup(options);
        check(pbe::verify_snapshot(repo, parallel.id).bytes == first.logical_bytes, "parallel worker restore data integrity");
        const auto parallel_restore = root / ("restored-w" + std::to_string(workers));
        pbe::restore_snapshot({repo, parallel_restore, parallel.id});
        check(same_tree(source, parallel_restore), "parallel worker output restores to identical tree");
    }

    std::vector<std::uint8_t> updated(1u << 20, 'X');
    write_bytes(source / "folder" / "beta.txt", updated);
    options.trust_metadata = true;
    const auto second = pbe::backup(options);
    check(second.id != first.id, "new snapshot id");
    check(second.read_bytes == (1u << 20), "trusted metadata reuses unchanged file maps without rereading them");
    const auto second_restore = root / "restored-second";
    pbe::restore_snapshot({repo, second_restore, second.id});
    check(same_tree(source, second_restore), "incremental snapshot restores latest content");
    check(pbe::list_snapshots(repo).size() == 5, "list complete snapshots");
    check(pbe::show_snapshot(repo, first.id).files == 6, "show old snapshot");
    const auto stats = pbe::repository_stats(repo);
    check(stats.snapshots == 5 && stats.object_count >= 3, "repository stats");
    options.source = root;
    bool recursive_rejected = false;
    try { (void)pbe::backup(options); } catch (...) { recursive_rejected = true; }
    check(recursive_rejected, "backup refuses source and repository overlap");
    fs::remove_all(root);
}

void parallel_object_writer_round_trip() {
    const auto root = fs::temp_directory_path() / ("pbe-writer-pool-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const auto repo = root / "repo";
    const auto source = root / "source";
    fs::create_directories(source);
    for (std::size_t index = 0; index < 8; ++index)
        write_bytes(source / ("unique-" + std::to_string(index) + ".bin"),
                    std::vector<std::uint8_t>(1u << 20, static_cast<std::uint8_t>('A' + index)));
    pbe::init_repository(repo);
    pbe::BackupOptions options;
    options.repo = repo;
    options.source = source;
    options.workers = 8;
    options.writer_workers = 4;
    options.memory_mib = 32;
    options.test_writer_delay_ms = 20;
    const auto snapshot = pbe::backup(options);
    check(snapshot.writer_workers == 4, "backup summary reports the object writer pool size");
    check(snapshot.files == 8 && snapshot.logical_bytes == 8ull * (1u << 20),
          "parallel object writers publish all source entries");
    check(pbe::verify_snapshot(repo, snapshot.id).bytes == snapshot.logical_bytes,
          "parallel object writer output passes full verification");
    fs::remove_all(root);
}

void pack_v2_round_trip() {
    const auto root = fs::temp_directory_path() / ("pbe-pack-v2-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const auto repo = root / "repo";
    const auto source = root / "source";
    const auto restored = root / "restored";
    fs::create_directories(source);
    std::vector<std::uint8_t> a(1u << 20, 'A');
    std::vector<std::uint8_t> b(1u << 20, 'B');
    std::vector<std::uint8_t> c(1u << 20);
    for (std::size_t i = 0; i < c.size(); ++i) c[i] = static_cast<std::uint8_t>((i * 131u + i / 17u) & 0xffu);
    std::vector<std::uint8_t> first_file;
    first_file.insert(first_file.end(), a.begin(), a.end());
    first_file.insert(first_file.end(), a.begin(), a.end());
    first_file.insert(first_file.end(), b.begin(), b.end());
    std::vector<std::uint8_t> second_file;
    second_file.insert(second_file.end(), a.begin(), a.end());
    second_file.insert(second_file.end(), c.begin(), c.end());
    write_bytes(source / "first.bin", first_file);
    write_bytes(source / "second.bin", second_file);

    pbe::init_repository(repo, "pack-v2");
    pbe::BackupOptions options;
    options.repo = repo; options.source = source; options.workers = 4; options.writer_workers = 1; options.memory_mib = 8;
    const auto first = pbe::backup(options);
    check(first.memory_credit_peak_bytes <= options.memory_mib * 1024u * 1024u,
          "pack batching holds memory credits within the configured budget");
    check(first.unique_raw_bytes == 3u * (1u << 20) && first.reused_chunks == 2,
          "in-flight duplicate chunks reuse the first durable pack object before metadata commit");
    check(pbe::verify_snapshot(repo, first.id).bytes == first.logical_bytes,
          "pack-v2 full verification checks object headers, locations, and content hashes");
    const auto stats = pbe::repository_stats(repo);
    check(stats.object_count == 3 && stats.referenced_objects == 3,
          "pack-v2 catalog indexes each unique object exactly once");
    bool found_pack = false;
    for (const auto& item : fs::directory_iterator(repo / "objects" / "packs")) {
        if (item.path().extension() == ".pack") found_pack = true;
    }
    check(found_pack, "pack-v2 publishes immutable pack files");
    bool found_loose_object = false;
    for (const auto& item : fs::recursive_directory_iterator(repo / "objects"))
        if (item.path().extension() == ".obj") found_loose_object = true;
    check(!found_loose_object, "pack-v2 stores objects in packs instead of per-object files");
    pbe::restore_snapshot({repo, restored, first.id});
    check(same_tree(source, restored), "pack-v2 restores the full source tree");

    options.trust_metadata = true;
    const auto second = pbe::backup(options);
    check(second.read_bytes == 0 && pbe::verify_snapshot(repo, second.id).bytes == second.logical_bytes,
          "pack-v2 trusted metadata cache reuses and verifies packed object locations");
    fs::remove_all(root);
}





void chunk_boundaries() {
    constexpr std::size_t chunk_size = 1u << 20;
    const auto root = fs::temp_directory_path() / ("pbe-chunk-boundaries-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const auto repo = root / "repo", source = root / "source", restored = root / "restored";
    fs::create_directories(source);
    const std::vector<std::size_t> lengths{0, 1, chunk_size - 1, chunk_size, chunk_size + 1};
    std::uint64_t expected_bytes = 0;
    for (std::size_t index = 0; index < lengths.size(); ++index) {
        std::vector<std::uint8_t> bytes(lengths[index]);
        for (std::size_t offset = 0; offset < bytes.size(); ++offset)
            bytes[offset] = static_cast<std::uint8_t>((offset * 37 + index * 53 + lengths[index]) & 0xff);
        write_bytes(source / ("boundary-" + std::to_string(index) + ".bin"), bytes);
        expected_bytes += bytes.size();
    }
    pbe::init_repository(repo);
    pbe::BackupOptions options; options.repo = repo; options.source = source; options.workers = 4; options.memory_mib = 16;
    const auto snapshot = pbe::backup(options);
    check(snapshot.files == lengths.size(), "all chunk-boundary files are recorded");
    check(snapshot.chunks == 5, "zero and one-mebibyte boundary chunk counts");
    const auto verification = pbe::verify_snapshot(repo, snapshot.id);
    check(verification.bytes == expected_bytes && verification.chunks == 5, "boundary snapshot verifies exact byte and chunk totals");
    pbe::restore_snapshot({repo, restored, snapshot.id});
    for (std::size_t index = 0; index < lengths.size(); ++index) {
        const auto name = "boundary-" + std::to_string(index) + ".bin";
        check(read_bytes(source / name) == read_bytes(restored / name), "chunk-boundary file round trip");
    }
    fs::remove_all(root);
}

void initialization_refuses_existing_data() {
    const auto root = fs::temp_directory_path() / ("pbe-init-safety-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::create_directories(root);
    const auto sentinel = root / "keep.txt";
    write_bytes(sentinel, std::vector<std::uint8_t>{'s', 'a', 'f', 'e'});
    bool refused = false;
    try { pbe::init_repository(root); } catch (...) { refused = true; }
    check(refused && read_bytes(sentinel) == std::vector<std::uint8_t>({'s', 'a', 'f', 'e'}),
          "init refuses a non-empty directory without touching existing data");
    fs::remove_all(root);
}

void codec_fallback_round_trip() {
    const auto root = fs::temp_directory_path() / ("pbe-codecs-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const auto repo = root / "repo", source = root / "source", restored = root / "restored";
    fs::create_directories(source);
    std::vector<std::uint8_t> random_bytes(1u << 20);
    std::mt19937 generator(0x504245u);
    for (auto& byte : random_bytes) byte = static_cast<std::uint8_t>(generator() & 0xffu);
    write_bytes(source / "random.bin", random_bytes);
    write_bytes(source / "compressible.bin", std::vector<std::uint8_t>(1u << 20, 0x5a));
    pbe::init_repository(repo);
    pbe::BackupOptions options; options.repo = repo; options.source = source; options.workers = 4; options.memory_mib = 16;
    const auto snapshot = pbe::backup(options);
    bool saw_raw = false, saw_zstd = false;
    for (const auto& item : fs::recursive_directory_iterator(repo / "objects")) {
        if (!item.is_regular_file() || item.path().extension() != ".obj") continue;
        const auto object = read_bytes(item.path());
        check(object.size() >= 64, "stored object includes complete header");
        if (object[10] == 0) {
            saw_raw = true;
            check(read_le(object, 16, 8) == read_le(object, 24, 8), "raw fallback stores payload without expansion");
        } else if (object[10] == 1) saw_zstd = true;
    }
    check(saw_raw && saw_zstd, "incompressible and compressible chunks use raw and zstd codecs");
    check(pbe::verify_snapshot(repo, snapshot.id).bytes == snapshot.logical_bytes, "both codec objects verify fully");
    pbe::restore_snapshot({repo, restored, snapshot.id});
    check(same_tree(source, restored), "raw and compressed files restore byte-for-byte");
    fs::remove_all(root);
}

void malformed_catalog_paths_are_rejected() {
    const auto root = fs::temp_directory_path() / ("pbe-bad-path-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const auto repo = root / "repo", source = root / "source";
    fs::create_directories(source);
    write_bytes(source / "empty", {});
    pbe::init_repository(repo);
    pbe::BackupOptions options; options.repo = repo; options.source = source; options.workers = 1; options.memory_mib = 8;
    const auto snapshot = pbe::backup(options);
    for (const std::string path : {"../escape", "/absolute"}) {
        update_catalog_text(repo, "UPDATE entries SET relative_path=? WHERE snapshot_id=?", path, snapshot.id);
        bool rejected = false;
        try { pbe::restore_snapshot({repo, root / "restore", snapshot.id}); } catch (...) { rejected = true; }
        check(rejected, "restore rejects traversal and absolute catalog paths");
    }
    const std::string nul_path("safe\0../escape", 14);
    update_catalog_text(repo, "UPDATE entries SET relative_path=? WHERE snapshot_id=?", nul_path, snapshot.id);
    bool rejected = false;
    try { pbe::restore_snapshot({repo, root / "restore", snapshot.id}); } catch (...) { rejected = true; }
    check(rejected, "restore preserves and rejects embedded NUL in catalog paths");
    fs::remove_all(root);
}

void malformed_catalog_object_ids_are_rejected() {
    const auto root = fs::temp_directory_path() / ("pbe-bad-object-id-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const auto repo = root / "repo", source = root / "source", target = root / "restore";
    fs::create_directories(source);
    write_bytes(source / "data", std::vector<std::uint8_t>{1, 2, 3});
    pbe::init_repository(repo);
    pbe::BackupOptions options; options.repo = repo; options.source = source; options.workers = 1; options.memory_mib = 8;
    const auto snapshot = pbe::backup(options);
    const std::string traversal_id = "../" + std::string(61, 'a');
    check(traversal_id.size() == 64, "crafted object identifier has format length");
    update_catalog_text(repo, "UPDATE entry_chunks SET object_id=? WHERE snapshot_id=?", traversal_id, snapshot.id);
    bool rejected = false;
    try { pbe::restore_snapshot({repo, target, snapshot.id}); } catch (...) { rejected = true; }
    check(rejected, "restore rejects non-hex object IDs before constructing object paths");
    fs::remove_all(root);
}

void unsupported_fifo_behavior() {
    const auto root = fs::temp_directory_path() / ("pbe-fifo-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const auto repo = root / "repo", source = root / "source";
    fs::create_directories(source);
    write_bytes(source / "ordinary", std::vector<std::uint8_t>{1, 2, 3});
    check(::mkfifo((source / "pipe").c_str(), 0600) == 0, "create FIFO fixture");
    pbe::init_repository(repo);
    pbe::BackupOptions options; options.repo = repo; options.source = source; options.workers = 1; options.memory_mib = 8;
    bool refused = false;
    try { (void)pbe::backup(options); } catch (...) { refused = true; }
    check(refused && pbe::list_snapshots(repo).empty(), "unsupported FIFO fails without publishing a snapshot");
    options.skip_unsupported = true;
    const auto snapshot = pbe::backup(options);
    check(snapshot.files == 1 && pbe::verify_snapshot(repo, snapshot.id).bytes == 3,
          "explicit skip-unsupported records the ordinary file and omits FIFO");
    fs::remove_all(root);
}

void diagnostic_report_failure_is_nonfatal() {
    const auto root = fs::temp_directory_path() / ("pbe-report-failure-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const auto repo = root / "repo", source = root / "source";
    fs::create_directories(source);
    write_bytes(source / "data", std::vector<std::uint8_t>{1, 2, 3, 4});
    pbe::init_repository(repo);
    fs::remove_all(repo / "reports");
    write_bytes(repo / "reports", std::vector<std::uint8_t>{'x'});
    pbe::BackupOptions options; options.repo = repo; options.source = source; options.workers = 2; options.memory_mib = 8;
    const auto snapshot = pbe::backup(options);
    check(pbe::verify_snapshot(repo, snapshot.id).bytes == 4,
          "optional diagnostic report failure does not prevent a correct snapshot");
    fs::remove_all(root);
}

void process_interrupt_preserves_old_snapshot() {
    const auto root = fs::temp_directory_path() / ("pbe-interrupt-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const auto repo = root / "repo", source = root / "source";
    fs::create_directories(source);
    std::vector<std::uint8_t> contents(48u * 1024u * 1024u);
    std::mt19937 generator(0x1a2b3c4du);
    for (auto& byte : contents) byte = static_cast<std::uint8_t>(generator() & 0xffu);
    write_bytes(source / "data.bin", contents);
    pbe::init_repository(repo);
    pbe::BackupOptions options; options.repo = repo; options.source = source; options.workers = 4; options.memory_mib = 16;
    const auto original = pbe::backup(options);

    int ready[2]{};
    check(::pipe(ready) == 0, "create child readiness pipe");
    const pid_t child = ::fork();
    check(child >= 0, "fork interrupt test child");
    if (child == 0) {
        ::close(ready[0]);
        std::signal(SIGINT, interrupt_handler);
        const char started = 'R';
        (void)::write(ready[1], &started, 1);
        ::close(ready[1]);
        options.adaptive = true;
        options.workers = 8;
        options.max_workers = 8;
        options.test_cpu_spin_ms = 200;
        try { (void)pbe::backup(options); std::_Exit(10); }
        catch (const std::exception& error) {
            const std::string message = error.what();
            std::_Exit(message.find("cancelled") != std::string::npos ? 0 : 11);
        }
    }
    ::close(ready[1]);
    char started = 0;
    const ssize_t got = ::read(ready[0], &started, 1);
    ::close(ready[0]);
    check(got == 1 && started == 'R', "interrupt child reached backup entry");
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    check(::kill(child, SIGINT) == 0, "send SIGINT to active backup");
    int status = 0;
    bool exited = false;
    for (int attempt = 0; attempt < 500; ++attempt) {
        const pid_t waited = ::waitpid(child, &status, WNOHANG);
        if (waited == child) { exited = true; break; }
        check(waited == 0, "wait for interrupted backup child");
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (!exited) {
        (void)::kill(child, SIGKILL);
        (void)::waitpid(child, &status, 0);
    }
    check(exited && WIFEXITED(status) && WEXITSTATUS(status) == 0,
          "SIGINT cancellation joins workers and returns promptly");
    check(pbe::list_snapshots(repo).size() == 1 && pbe::verify_snapshot(repo, original.id).bytes == contents.size(),
          "interrupted backup leaves old complete snapshot readable and publishes no partial snapshot");
    fs::remove_all(root);
}

void source_change_is_rejected() {
    const auto root = fs::temp_directory_path() / ("pbe-mutation-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const auto repo = root / "repo", source = root / "source";
    fs::create_directories(source);
    const auto file = source / "changing.bin";
    write_bytes(file, std::vector<std::uint8_t>(32u * 1024u * 1024u, 'A'));
    pbe::init_repository(repo);
    pbe::BackupOptions options; options.repo = repo; options.source = source; options.workers = 4;
    options.memory_mib = 16; options.test_reader_delay_ms = 20;
    std::exception_ptr failure;
    std::thread backup_thread([&] { try { (void)pbe::backup(options); } catch (...) { failure = std::current_exception(); } });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    {
        std::fstream out(file, std::ios::in | std::ios::out | std::ios::binary);
        out.seekp(0); out.put('Z'); out.flush();
    }
    backup_thread.join();
    check(static_cast<bool>(failure), "mutated source fails backup");
    check(pbe::list_snapshots(repo).empty(), "mutated source does not publish snapshot");
    fs::remove_all(root);
}

void object_corruption_is_detected() {
    const auto root = fs::temp_directory_path() / ("pbe-corrupt-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const auto repo = root / "repo", source = root / "source";
    fs::create_directories(source);
    write_bytes(source / "data", std::vector<std::uint8_t>(10000, 'C'));
    pbe::init_repository(repo);
    pbe::BackupOptions options; options.repo = repo; options.source = source; options.workers = 2; options.memory_mib = 8;
    const auto snapshot = pbe::backup(options);
    fs::path object;
    for (const auto& item : fs::recursive_directory_iterator(repo / "objects")) if (item.path().extension() == ".obj") { object = item.path(); break; }
    check(!object.empty(), "object file exists");
    const auto original = read_bytes(object);
    check(original.size() > 64, "object includes payload bytes");
    std::vector<std::vector<std::uint8_t>> damaged;
    auto bad = original; bad[0] ^= 0x01; damaged.push_back(bad);
    bad = original; bad[8] = 2; damaged.push_back(bad);
    bad = original; bad[10] = 2; damaged.push_back(bad);
    bad = original; bad[11] = 1; damaged.push_back(bad);
    bad = original; std::fill(bad.begin() + 16, bad.begin() + 24, 0); bad[18] = 0x20; damaged.push_back(bad);
    bad = original; const auto stored = read_le(original, 24, 8) + 1;
    for (std::size_t i = 0; i < 8; ++i) bad[24 + i] = static_cast<std::uint8_t>(stored >> (i * 8));
    damaged.push_back(bad);
    bad = original; bad.back() ^= 0x5a; damaged.push_back(bad);
    for (const auto& bytes : damaged) {
        write_bytes(object, bytes);
        bool rejected = false;
        try { (void)pbe::verify_snapshot(repo, snapshot.id); } catch (...) { rejected = true; }
        check(rejected, "verification rejects corrupt object magic, version, codec, flags, length, or payload");
    }
    write_bytes(object, damaged.back());
    bool backup_rejected = false;
    try { (void)pbe::backup(options); } catch (...) { backup_rejected = true; }
    check(backup_rejected && pbe::list_snapshots(repo).size() == 1,
          "backup refuses to publish a snapshot that reuses a corrupt existing object");
    fs::remove_all(root);
}

void writer_failure_releases_duplicate_waiters() {
    const auto root = fs::temp_directory_path() / ("pbe-writer-failure-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const auto repo = root / "repo", source = root / "source";
    fs::create_directories(source);
    const std::vector<std::uint8_t> repeated_chunk(1u << 20, 'W');
    std::vector<std::uint8_t> repeated_file;
    for (int i = 0; i < 8; ++i) repeated_file.insert(repeated_file.end(), repeated_chunk.begin(), repeated_chunk.end());
    write_bytes(source / "repeated.bin", repeated_file);
    pbe::init_repository(repo);
    const auto id = pbe::digest_hex(pbe::blake3(repeated_chunk));
    const auto shard = repo / "objects" / id.substr(0, 2);
    fs::create_directories(shard);
    fs::create_symlink("missing-object-target", shard / (id + ".obj"));
    pbe::BackupOptions options; options.repo = repo; options.source = source; options.workers = 8;
    options.memory_mib = 32; options.test_cpu_spin_ms = 150;
    const auto start = std::chrono::steady_clock::now();
    bool failed = false;
    try { (void)pbe::backup(options); } catch (...) { failed = true; }
    const auto elapsed = std::chrono::steady_clock::now() - start;
    check(failed, "broken object path makes the blob writer fail");
    check(elapsed < std::chrono::seconds(10), "writer error wakes duplicate-claim followers without deadlock");
    check(pbe::list_snapshots(repo).empty(), "writer failure never publishes a partial snapshot");
    fs::remove_all(root);
}

void crash_publish_matrix() {
    for (const std::string storage_format : {"files-v1", "pack-v2"}) {
      for (const std::string failpoint : {"after-temp-write", "after-object-publish", "before-sqlite-batch-commit", "before-final-publish", "after-final-publish"}) {
        const auto root = fs::temp_directory_path() / ("pbe-crash-" + storage_format + "-" + failpoint + "-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        const auto repo = root / "repo";
        const auto source = root / "source";
        fs::create_directories(source);
        write_bytes(source / "data.bin", std::vector<std::uint8_t>(1u << 20, 'A'));
        pbe::init_repository(repo, storage_format);
        pbe::BackupOptions options; options.repo = repo; options.source = source; options.workers = 2; options.memory_mib = 16;
        const auto original = pbe::backup(options);
        write_bytes(source / "data.bin", std::vector<std::uint8_t>(1u << 20, 'B'));
        const pid_t child = ::fork();
        check(child >= 0, "fork for failpoint test");
        if (child == 0) {
            options.failpoint = failpoint;
            (void)pbe::backup(options);
            std::_Exit(10);
        }
        int status = 0;
        check(::waitpid(child, &status, 0) == child, "wait for failpoint child");
        check(WIFEXITED(status) && WEXITSTATUS(status) == 86, "failpoint child exits at requested checkpoint");
        const auto snapshots = pbe::list_snapshots(repo);
        const bool published = failpoint == "after-final-publish";
        check(snapshots.size() == (published ? 2u : 1u), "crash never exposes a partial snapshot");
        check(pbe::verify_snapshot(repo, original.id).bytes == (1u << 20), "previous complete snapshot survives crash");
        if (published) check(pbe::verify_snapshot(repo, snapshots.front().id).bytes == (1u << 20), "post-publish snapshot is complete");
        options.failpoint.clear();
        const auto recovered = pbe::backup(options);
        check(pbe::verify_snapshot(repo, recovered.id).bytes == (1u << 20), "next writer recovers and publishes after process crash");
        check(pbe::list_snapshots(repo).size() == (published ? 3u : 2u), "recovery exposes only complete snapshots");
        check(fs::is_empty(repo / "staging"), "recovery removes abandoned staging contents");
        if (storage_format == "pack-v2") {
            sqlite3* db = nullptr;
            check(sqlite3_open_v2((repo / "catalog.sqlite").c_str(), &db, SQLITE_OPEN_READONLY, nullptr) == SQLITE_OK,
                  "open pack catalog after recovery");
            sqlite3_stmt* statement = nullptr;
            check(sqlite3_prepare_v2(db, "SELECT DISTINCT pack_id FROM objects WHERE pack_id IS NOT NULL", -1,
                                     &statement, nullptr) == SQLITE_OK, "query indexed packs after recovery");
            std::set<std::string> indexed, present;
            while (sqlite3_step(statement) == SQLITE_ROW) {
                const auto* value = sqlite3_column_text(statement, 0);
                indexed.emplace(reinterpret_cast<const char*>(value));
            }
            sqlite3_finalize(statement);
            sqlite3_close(db);
            for (const auto& item : fs::directory_iterator(repo / "objects" / "packs"))
                if (item.path().extension() == ".pack") present.insert(item.path().stem().string());
            check(indexed == present, "recovery removes packs published before a catalog commit");
        }
        fs::remove_all(root);
      }
    }
}
}

int main() {
    try {
        std::cout << "[TEST] hash_vectors\n" << std::flush;
        hash_vectors();
        std::cout << "[TEST] queue_contract\n" << std::flush;
        queue_contract();
        std::cout << "[TEST] concurrency_contract\n" << std::flush;
        concurrency_contract();
        std::cout << "[TEST] diagnosis_rules\n" << std::flush;
        diagnosis_rules();
        std::cout << "[TEST] adaptive_controller_policy\n" << std::flush;
        adaptive_controller_policy();
        std::cout << "[TEST] object_store_contract\n" << std::flush;
        object_store_contract();
        std::cout << "[TEST] initialization_refuses_existing_data\n" << std::flush;
        initialization_refuses_existing_data();
        std::cout << "[TEST] backup_round_trip\n" << std::flush;
        backup_round_trip();
        std::cout << "[TEST] parallel_object_writer_round_trip\n" << std::flush;
        parallel_object_writer_round_trip();
        std::cout << "[TEST] pack_v2_round_trip\n" << std::flush;
        pack_v2_round_trip();
        std::cout << "[TEST] chunk_boundaries\n" << std::flush;
        chunk_boundaries();
        std::cout << "[TEST] codec_fallback_round_trip\n" << std::flush;
        codec_fallback_round_trip();
        std::cout << "[TEST] malformed_catalog_paths_are_rejected\n" << std::flush;
        malformed_catalog_paths_are_rejected();
        std::cout << "[TEST] malformed_catalog_object_ids_are_rejected\n" << std::flush;
        malformed_catalog_object_ids_are_rejected();
        std::cout << "[TEST] unsupported_fifo_behavior\n" << std::flush;
        unsupported_fifo_behavior();
        std::cout << "[TEST] diagnostic_report_failure_is_nonfatal\n" << std::flush;
        diagnostic_report_failure_is_nonfatal();
        std::cout << "[TEST] process_interrupt_preserves_old_snapshot\n" << std::flush;
        process_interrupt_preserves_old_snapshot();
        std::cout << "[TEST] source_change_is_rejected\n" << std::flush;
        source_change_is_rejected();
        std::cout << "[TEST] object_corruption_is_detected\n" << std::flush;
        object_corruption_is_detected();
        std::cout << "[TEST] writer_failure_releases_duplicate_waiters\n" << std::flush;
        writer_failure_releases_duplicate_waiters();
        std::cout << "[TEST] crash_publish_matrix\n" << std::flush;
        crash_publish_matrix();
        std::cout << "PBE tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "PBE tests failed: " << error.what() << '\n';
        return 1;
    }
}
