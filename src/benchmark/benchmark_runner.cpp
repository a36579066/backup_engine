#include "pbe/engine.hpp"
#include "storage/repository.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <sys/utsname.h>
#include <thread>
#include <vector>

namespace pbe {
namespace {
using detail::canonical_existing;
using detail::path_within;

std::string random_id(std::string_view prefix) {
    std::random_device rd;
    std::mt19937_64 rng((std::uint64_t(rd()) << 32) ^ rd() ^
                        static_cast<std::uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count()));
    const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    std::ostringstream out;
    out << prefix << now << '-' << std::hex << std::setfill('0') << std::setw(12) << (rng() & 0xffffffffffffull);
    return out.str();
}

std::string json_escape(std::string_view value) {
    std::ostringstream out;
    for (unsigned char c : value) {
        switch (c) {
        case '"': out << "\\\""; break;
        case '\\': out << "\\\\"; break;
        case '\b': out << "\\b"; break;
        case '\f': out << "\\f"; break;
        case '\n': out << "\\n"; break;
        case '\r': out << "\\r"; break;
        case '\t': out << "\\t"; break;
        default:
            if (c < 0x20) out << "\\u00" << std::hex << std::setw(2) << std::setfill('0') << unsigned(c) << std::dec;
            else out << static_cast<char>(c);
        }
    }
    return out.str();
}

std::string system_name() {
    struct utsname info{};
    if (::uname(&info) == 0) return std::string(info.sysname) + " " + info.release + " " + info.machine;
    return "unknown";
}
} // namespace

std::string run_benchmark(const BenchmarkOptions& options) {
    if (options.dataset.empty() || options.workers.empty() || options.schedulers.empty() || options.repeat == 0 || options.repeat > 1000)
        throw std::runtime_error("benchmark requires a dataset, worker counts, schedulers, and repeat in 1..1000");
    if (options.writer_workers == 0 || options.writer_workers > 32)
        throw std::runtime_error("benchmark writer workers must be in 1..32");
    if (options.storage_format != "files-v1" && options.storage_format != "pack-v2")
        throw std::runtime_error("benchmark storage format must be files-v1 or pack-v2");
    const auto dataset = canonical_existing(options.dataset);
    if (!std::filesystem::is_directory(dataset)) throw std::runtime_error("benchmark dataset must be a directory");
    for (const auto workers : options.workers) if (!workers || workers > 256) throw std::runtime_error("benchmark workers must be in 1..256");
    for (const auto& scheduler : options.schedulers) if (scheduler != "fixed" && scheduler != "adaptive") throw std::runtime_error("benchmark scheduler must be fixed or adaptive");
    auto output = std::filesystem::absolute(options.output).lexically_normal();
    if (path_within(output, dataset)) throw std::runtime_error("benchmark output must be outside the dataset tree");
    std::filesystem::create_directories(output);
    const auto temporary = std::filesystem::temp_directory_path() / random_id("pbe-benchmark-");
    std::filesystem::create_directories(temporary);
    struct Run {
        std::string scheduler;
        std::size_t workers = 0, writer_workers = 1, repeat = 0;
        double seconds = 0, controller_seconds = 0;
        std::uint64_t logical = 0, read = 0, unique_raw = 0, stored = 0;
        std::uint64_t reused_chunks = 0;
        std::size_t memory_peak = 0, rss_peak = 0, worker_churn = 0;
    };
    std::vector<Run> runs;
    try {
        for (const auto& scheduler : options.schedulers) {
            for (const auto workers : options.workers) {
                for (std::size_t repeat = 1; repeat <= options.repeat; ++repeat) {
                    const auto repo = temporary / (scheduler + "-w" + std::to_string(workers) + "-ow" + std::to_string(options.writer_workers) + "-r" + std::to_string(repeat));
                    init_repository(repo, options.storage_format);
                    BackupOptions backup_options;
                    backup_options.repo = repo; backup_options.source = dataset;
                    backup_options.workers = workers; backup_options.max_workers = workers;
                    backup_options.writer_workers = options.writer_workers;
                    backup_options.memory_mib = options.memory_mib; backup_options.adaptive = scheduler == "adaptive";
                    backup_options.test_writer_delay_ms = options.test_writer_delay_ms;
                    backup_options.test_reader_delay_ms = options.test_reader_delay_ms;
                    backup_options.test_cpu_spin_ms = options.test_cpu_spin_ms;
                    BackupSummary summary;
                    const auto base = dataset / "base", version2 = dataset / "version-2";
                    if (std::filesystem::is_directory(base) && std::filesystem::is_directory(version2)) {
                        backup_options.source = base;
                        const auto baseline = backup(backup_options);
                        (void)verify_snapshot(repo, baseline.id);
                        backup_options.source = version2;
                        summary = backup(backup_options);
                    } else {
                        summary = backup(backup_options);
                    }
                    (void)verify_snapshot(repo, summary.id);
                    runs.push_back(Run{scheduler, workers, options.writer_workers, repeat, summary.elapsed_seconds,
                        summary.controller_overhead_seconds, summary.logical_bytes, summary.read_bytes,
                        summary.unique_raw_bytes, summary.new_stored_bytes, summary.reused_chunks,
                        summary.memory_credit_peak_bytes, summary.process_rss_peak_bytes, summary.worker_churn});
                    std::error_code ec;
                    std::filesystem::remove_all(repo, ec);
                }
            }
        }
    } catch (...) {
        std::error_code ec; std::filesystem::remove_all(temporary, ec); throw;
    }
    std::error_code ec; std::filesystem::remove_all(temporary, ec);
    auto csv_path = output / "benchmark.csv";
    auto json_path = output / "benchmark.json";
    auto md_path = output / "benchmark.md";
    {
        std::ofstream csv(csv_path);
        csv << std::setprecision(12);
        csv << "storage_format,scheduler,workers,writer_workers,repeat,elapsed_seconds,logical_bytes,read_bytes,unique_raw_bytes,new_stored_bytes,reused_chunks,memory_credit_peak_bytes,process_rss_peak_bytes,worker_churn,controller_overhead_seconds\n";
        for (const auto& r : runs) csv << options.storage_format << ',' << r.scheduler << ',' << r.workers << ',' << r.writer_workers << ',' << r.repeat << ',' << r.seconds << ',' << r.logical << ',' << r.read << ',' << r.unique_raw << ',' << r.stored << ',' << r.reused_chunks << ',' << r.memory_peak << ',' << r.rss_peak << ',' << r.worker_churn << ',' << r.controller_seconds << '\n';
        if (!csv) throw std::runtime_error("cannot write benchmark CSV");
    }
    {
        std::ofstream json(json_path);
        json << std::setprecision(12);
        json << "{\"schema_version\":1,\"dataset\":\"" << json_escape(dataset.string()) << "\",\"system\":\"" << json_escape(system_name()) << "\",\"repeat\":" << options.repeat
             << ",\"hardware_concurrency\":" << std::thread::hardware_concurrency() << ",\"writer_workers\":" << options.writer_workers << ",\"memory_mib\":" << options.memory_mib
             << ",\"storage_format\":\"" << json_escape(options.storage_format) << "\""
             << ",\"test_writer_delay_ms\":" << options.test_writer_delay_ms << ",\"test_reader_delay_ms\":" << options.test_reader_delay_ms
             << ",\"test_cpu_spin_ms\":" << options.test_cpu_spin_ms << ",\"runs\":[";
        for (std::size_t i = 0; i < runs.size(); ++i) {
            if (i) json << ',';
            const auto& r = runs[i];
            json << "{\"scheduler\":\"" << r.scheduler << "\",\"workers\":" << r.workers << ",\"writer_workers\":" << r.writer_workers << ",\"repeat\":" << r.repeat
                 << ",\"elapsed_seconds\":" << r.seconds << ",\"logical_bytes\":" << r.logical
                 << ",\"read_bytes\":" << r.read << ",\"unique_raw_bytes\":" << r.unique_raw << ",\"new_stored_bytes\":" << r.stored
                 << ",\"reused_chunks\":" << r.reused_chunks
                 << ",\"memory_credit_peak_bytes\":" << r.memory_peak << ",\"process_rss_peak_bytes\":" << r.rss_peak
                 << ",\"worker_churn\":" << r.worker_churn
                 << ",\"controller_overhead_seconds\":" << r.controller_seconds << '}';
        }
        json << "]}\n";
        if (!json) throw std::runtime_error("cannot write benchmark JSON");
    }
    {
        std::ofstream md(md_path);
        md << "# PBE Benchmark Report\n\nDataset: `" << dataset.string() << "`  \nSystem: `" << system_name() << "`  \nStorage format: `" << options.storage_format << "`  \nRepeats per profile: " << options.repeat
           << "\n\nEach run used a new repository. Filesystem cache state and machine load were not controlled; treat these as local observations. No speedup is implied without inspecting the raw runs.\n\n"
           << "\nObject writer threads: " << options.writer_workers << "\n\n"
           << "| Scheduler | Workers | Object Writers | Runs | Median seconds | Stddev | Min | Max | Max regression | Peak credits | Peak RSS | Churn | Controller seconds | Logical bytes | Read bytes | Unique raw | Stored bytes | Reused chunks |\n"
           << "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n";
        for (const auto& scheduler : options.schedulers) for (const auto workers : options.workers) {
            std::vector<double> values;
            std::uint64_t logical = 0, read = 0, unique_raw = 0, stored = 0;
            std::uint64_t reused = 0;
            std::size_t memory_peak = 0, rss_peak = 0, churn = 0;
            double controller = 0;
            for (const auto& r : runs) if (r.scheduler == scheduler && r.workers == workers) {
                values.push_back(r.seconds); logical = r.logical; read += r.read; unique_raw += r.unique_raw; stored += r.stored; reused += r.reused_chunks;
                memory_peak = std::max(memory_peak, r.memory_peak); rss_peak = std::max(rss_peak, r.rss_peak);
                churn += r.worker_churn; controller += r.controller_seconds;
            }
            std::sort(values.begin(), values.end());
            const double median = values.empty() ? 0 : (values.size() % 2 ? values[values.size()/2] : (values[values.size()/2-1] + values[values.size()/2]) / 2);
            double mean = 0, variance = 0;
            for (double value : values) mean += value;
            if (!values.empty()) mean /= values.size();
            for (double value : values) variance += (value - mean) * (value - mean);
            const double stddev = values.empty() ? 0 : std::sqrt(variance / values.size());
            const double regression = values.empty() || values.front() == 0 ? 0 : values.back() / values.front() - 1;
            md << "| " << scheduler << " | " << workers << " | " << options.writer_workers << " | " << values.size() << " | " << median << " | " << stddev << " | "
               << (values.empty() ? 0 : values.front()) << " | " << (values.empty() ? 0 : values.back()) << " | " << regression * 100 << "% | "
               << memory_peak << " B | " << rss_peak << " B | " << churn << " | " << controller << " | " << logical << " | " << read << " | " << unique_raw << " | " << stored << " | " << reused << " |\n";
        }
        md << "\nPeak RSS is sampled from the process at 100 ms intervals; short-lived peaks can be missed. The `Max regression` column is the slowest run relative to the fastest run in that profile. The worst-case scheduler regression must be read by comparing fixed and adaptive runs at the same worker cap.\n\n"
           << "| Workers | Fixed median seconds | Adaptive median seconds | Adaptive delta vs fixed |\n|---:|---:|---:|---:|\n";
        for (const auto workers : options.workers) {
            auto median_for = [&](const std::string& scheduler) -> std::optional<double> {
                std::vector<double> values;
                for (const auto& r : runs) if (r.scheduler == scheduler && r.workers == workers) values.push_back(r.seconds);
                if (values.empty()) return std::nullopt;
                std::sort(values.begin(), values.end());
                return values.size() % 2 ? values[values.size()/2] : (values[values.size()/2-1] + values[values.size()/2]) / 2;
            };
            const auto fixed = median_for("fixed"), adaptive = median_for("adaptive");
            md << "| " << workers << " | " << (fixed ? std::to_string(*fixed) : "n/a") << " | "
               << (adaptive ? std::to_string(*adaptive) : "n/a") << " | ";
            if (fixed && adaptive && *fixed > 0) md << ((*adaptive / *fixed - 1) * 100) << "%";
            else md << "n/a";
            md << " |\n";
        }
        md << "\nRaw measurements: `benchmark.csv` and `benchmark.json`.\n";
        if (!md) throw std::runtime_error("cannot write benchmark Markdown");
    }
    return "Wrote " + csv_path.string() + ", " + json_path.string() + ", and " + md_path.string();
}
} // namespace pbe
