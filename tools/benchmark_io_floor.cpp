#include "core/backup_session.hpp"
#include "diagnostics/pipeline_monitor.hpp"
#include "pipeline/00_framework/pipeline_runtime.hpp"
#include "pipeline/01_reader/source_file.hpp"
#include "pipeline/03_storage/storage_stage.hpp"
#include "pipeline/shared/pipeline_context.hpp"
#include "pipeline/shared/stage_support.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/resource.h>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {
using namespace pbe::detail;
namespace fs = std::filesystem;

struct Arguments {
    std::string command;
    fs::path source;
    fs::path manifest;
    fs::path repo;
    std::size_t memory_mib = 256;
};

Arguments parse(int argc, char** argv) {
    if (argc < 2)
        throw std::runtime_error(
            "usage: pbe_io_floor prepare|run --source PATH --manifest PATH [--repo PATH]");
    Arguments args;
    args.command = argv[1];
    for (int i = 2; i < argc; i += 2) {
        if (i + 1 >= argc)
            throw std::runtime_error("missing argument value");
        const std::string key = argv[i];
        if (key == "--source")
            args.source = argv[i + 1];
        else if (key == "--manifest")
            args.manifest = argv[i + 1];
        else if (key == "--repo")
            args.repo = argv[i + 1];
        else if (key == "--memory-mib")
            args.memory_mib = std::stoull(argv[i + 1]);
        else
            throw std::runtime_error("unknown option: " + key);
    }
    if (args.source.empty() || args.manifest.empty())
        throw std::runtime_error("source and manifest are required");
    return args;
}

struct ExpectedChunk {
    std::string path;
    std::uint64_t ordinal = 0;
    std::uint64_t bytes = 0;
    pbe::Digest digest{};
};

// Precompute source digests outside timing so stored headers and later payload verification remain valid.
// Preparation is excluded from I/O timing; functional backups still need hashing.
void prepare_manifest(const Arguments& args) {
    std::atomic<bool> cancelled{false};
    const auto entries = scan_source(args.source, false, cancelled);
    std::ofstream out(args.manifest);
    if (!out)
        throw std::runtime_error("cannot create I/O manifest");
    out << "PBE-IO-MANIFEST-v1\n";
    std::unordered_set<std::string> unique;
    std::uint64_t chunks = 0;
    for (const auto& entry : entries) {
        if (entry.kind != "file")
            continue;
        SourceFile file(entry);
        for (std::uint64_t offset = 0, ordinal = 0; offset < entry.size; ++ordinal) {
            const auto bytes = std::min<std::uint64_t>(kChunkSize, entry.size - offset);
            std::vector<std::uint8_t> raw(bytes);
            file.read_into(raw);
            const auto digest = pbe::digest_hex(pbe::blake3(raw));
            if (!unique.insert(digest).second)
                throw std::runtime_error(
                    "I/O fixture must contain unique chunks for a fair comparison");
            out << std::quoted(entry.relative) << ' ' << ordinal << ' ' << bytes << ' ' << digest
                << '\n';
            offset += bytes;
            ++chunks;
        }
        file.validate_unchanged();
    }
    if (!out)
        throw std::runtime_error("cannot write I/O manifest");
    std::cout << "Prepared " << chunks << " unique chunk digests outside benchmark timing.\n";
}

std::vector<ExpectedChunk> load_manifest(const fs::path& path) {
    std::ifstream in(path);
    std::string magic;
    std::getline(in, magic);
    if (magic != "PBE-IO-MANIFEST-v1")
        throw std::runtime_error("invalid I/O manifest");
    std::vector<ExpectedChunk> chunks;
    while (in >> std::ws && in.peek() != std::char_traits<char>::eof()) {
        ExpectedChunk chunk;
        std::string hex;
        if (!(in >> std::quoted(chunk.path) >> chunk.ordinal >> chunk.bytes >> hex))
            throw std::runtime_error("malformed I/O manifest row");
        chunk.digest = pbe::digest_from_hex(hex);
        chunks.push_back(std::move(chunk));
    }
    if (chunks.empty())
        throw std::runtime_error("empty I/O manifest");
    return chunks;
}

struct ProbeState {
    PipelineContext& context;
    const std::vector<ExpectedChunk>& expected;
    // Only the single Writer callback mutates this state; the caller reads it after joining.
    std::vector<ObjectHeader> written;

    void record_completion(const WriteAck* ack, std::exception_ptr error) {
        if (error) {
            context.fail(error);
            return;
        }
        if (!ack)
            throw std::runtime_error("I/O writer returned no acknowledgement");
        written.push_back(ack->header);
        context.metrics.completed_chunks.fetch_add(1, std::memory_order_relaxed);
        context.metrics.completed_raw_bytes.fetch_add(ack->header.raw_bytes,
                                                      std::memory_order_relaxed);
    }

    void read_source() {
        const auto scan_begin = PipelineClock::now();
        const auto entries = scan_source(context.source, false, context.errors.cancelled);
        context.metrics.source_scan_ns.fetch_add(elapsed_ns(scan_begin), std::memory_order_relaxed);
        std::size_t next = 0;
        for (const auto& entry : entries) {
            if (entry.kind == "directory")
                continue;
            if (entry.kind != "file")
                throw std::runtime_error(
                    "I/O fixture must contain only regular files and directories");
            SourceFile file(entry);
            for (std::uint64_t offset = 0, ordinal = 0; offset < entry.size; ++ordinal) {
                const auto bytes = std::min<std::uint64_t>(kChunkSize, entry.size - offset);
                if (next >= expected.size())
                    throw std::runtime_error("source has more chunks than the manifest");
                const auto& ref = expected[next++];
                if (ref.path != entry.relative || ref.ordinal != ordinal || ref.bytes != bytes)
                    throw std::runtime_error("source layout differs from I/O manifest");

                // Acquire credit before allocation, using the same queue/credit backpressure as the pipeline.
                auto credit =
                    context.memory.acquire(context.credit_per_chunk, context.errors.cancelled);
                BlobWriteTask task;
                task.digest = ref.digest;
                task.raw_bytes = bytes;
                task.stored.resize(bytes);
                const auto read_begin = PipelineClock::now();
                file.read_into(task.stored);
                context.metrics.source_read_ns.fetch_add(elapsed_ns(read_begin),
                                                         std::memory_order_relaxed);
                task.retained_credit = std::make_shared<MemoryBudget::Credit>(std::move(credit));
                task.completion = [this](const WriteAck* ack, std::exception_ptr error) {
                    record_completion(ack, error);
                };
                if (!context.object_queue.push(std::move(task), &context.errors.cancelled)) {
                    context.errors.rethrow();
                    throw std::runtime_error("I/O writer queue closed");
                }
                offset += bytes;
            }
            file.validate_unchanged();
        }
        if (next != expected.size())
            throw std::runtime_error("source has fewer chunks than the manifest");
    }

    std::uint64_t verify() const {
        if (written.size() != expected.size())
            throw std::runtime_error("I/O writer did not acknowledge every chunk");
        std::uint64_t bytes = 0;
        for (std::size_t i = 0; i < written.size(); ++i) {
            const auto& header = written[i];
            const auto& ref = expected[i];
            if (header.digest != ref.digest || header.raw_bytes != ref.bytes)
                throw std::runtime_error("I/O acknowledgement order/content mismatch");
            const auto raw =
                read_packed_object(context.repo, ObjectLocation{header.pack_id, header.pack_offset},
                                   pbe::digest_hex(ref.digest));
            if (raw.size() != ref.bytes)
                throw std::runtime_error("I/O payload verification failed");
            bytes += raw.size();
        }
        return bytes;
    }
};

double cpu_seconds(const timeval& time) {
    return static_cast<double>(time.tv_sec) + static_cast<double>(time.tv_usec) / 1e6;
}

void run_probe(const Arguments& args) {
    const auto manifest = load_manifest(args.manifest);
    pbe::BackupOptions options;
    options.repo = args.repo;
    options.source = args.source;
    options.workers = 4;
    options.writer_workers = 1;
    options.memory_mib = args.memory_mib;
    BackupSession session(make_backup_plan(options));
    if (!session.plan.packed_storage)
        throw std::runtime_error("I/O probe requires a fresh pack-v2 repository");
    fs::create_directories(session.staging_dir);
    PipelineContext context(session);
    ProbeState probe{context, manifest, {}};
    probe.written.reserve(manifest.size());
    std::atomic<bool> stop{false};
    PipelineRuntime runtime(context.errors, [&] {
        context.gate.close();
    });
    runtime.set_source({.name = "raw source", .run = [&] {
                            probe.read_source();
                        }});
    runtime.add_stage({.name = "production storage",
                       .workers = 1,
                       .run =
                           [&](std::size_t) {
                               run_pack_object_writer(context);
                           },
                       .close_input =
                           [&] {
                               context.object_queue.close();
                           }});
    runtime.add_observer({.name = "monitor",
                          .run =
                              [&] {
                                  run_pipeline_monitor(context, stop);
                              },
                          .request_stop =
                              [&] {
                                  stop.store(true, std::memory_order_relaxed);
                              }});

    // Reuse Pack Writer: 128 MiB target, eight-object writev, 2 ms idle flush, sync, and publication.
    // The timed reference omits hashing, compression, claims, and catalog commits; it is not a full backup.
    rusage before{}, after{};
    if (::getrusage(RUSAGE_SELF, &before) != 0)
        throw std::runtime_error("getrusage failed");
    const auto begin = PipelineClock::now();
    runtime.run();
    const double seconds = std::chrono::duration<double>(PipelineClock::now() - begin).count();
    if (::getrusage(RUSAGE_SELF, &after) != 0)
        throw std::runtime_error("getrusage failed");
    const auto verify_begin = PipelineClock::now();
    const auto bytes = probe.verify();
    const double verify_seconds =
        std::chrono::duration<double>(PipelineClock::now() - verify_begin).count();
    std::cout
        << std::setprecision(12) << "{\"backup_seconds\":" << seconds
        << ",\"verify_seconds\":" << verify_seconds
        << ",\"verified\":true,\"logical_bytes\":" << bytes << ",\"objects\":" << manifest.size()
        << ",\"user_cpu_seconds\":" << cpu_seconds(after.ru_utime) - cpu_seconds(before.ru_utime)
        << ",\"system_cpu_seconds\":" << cpu_seconds(after.ru_stime) - cpu_seconds(before.ru_stime)
        << ",\"memory_credit_peak_bytes\":" << context.memory.peak()
        << ",\"rss_peak_bytes\":" << context.metrics.process_rss_peak_bytes.load()
        << ",\"pack_files\":" << context.metrics.object_write_operation_count.load()
        << ",\"staging_write_calls\":" << context.metrics.staging_write_calls.load()
        << ",\"source_scan_seconds\":" << context.metrics.source_scan_ns.load() / 1e9
        << ",\"source_read_seconds\":" << context.metrics.source_read_ns.load() / 1e9
        << ",\"staging_write_seconds\":" << context.metrics.object_staging_write_ns.load() / 1e9
        << ",\"file_sync_seconds\":" << context.metrics.object_file_sync_ns.load() / 1e9
        << ",\"directory_sync_seconds\":" << context.metrics.object_directory_sync_ns.load() / 1e9
        << ",\"writer_total_seconds\":" << context.metrics.object_write_total_ns.load() / 1e9
        << ",\"written_bytes_with_object_headers\":" << context.metrics.new_stored_bytes.load()
        << ",\"run_id\":\"" << session.run_id << "\"}\n";
}
} // namespace

int main(int argc, char** argv) {
    try {
        const auto args = parse(argc, argv);
        if (args.command == "prepare")
            prepare_manifest(args);
        else if (args.command == "run" && !args.repo.empty())
            run_probe(args);
        else
            throw std::runtime_error("expected prepare or run with --repo");
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "pbe_io_floor: " << error.what() << '\n';
        return 1;
    }
}
