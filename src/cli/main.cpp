#include "pbe/engine.hpp"
#include "pbe/blake3.hpp"

#include <algorithm>
#include <charconv>
#include <csignal>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
struct Arguments {
    std::string command;
    std::map<std::string, std::string> values;
    std::set<std::string> flags;
};

[[noreturn]] void usage_error(const std::string& message) {
    throw std::invalid_argument(message);
}

Arguments parse(int argc, char** argv) {
    if (argc < 2) usage_error("missing command");
    Arguments result;
    result.command = argv[1];
    if (result.command == "--help" || result.command == "-h") result.command = "help";
    static const std::set<std::string> flag_options{
        "--json", "--full", "--skip-unsupported", "--paranoid", "--trust-metadata", "--help"};
    for (int i = 2; i < argc; ++i) {
        std::string token = argv[i];
        if (token == "--help" || token == "-h") { result.flags.insert("--help"); continue; }
        if (!token.starts_with("--")) usage_error("unexpected positional argument: " + token);
        const auto equal = token.find('=');
        const std::string key = equal == std::string::npos ? token : token.substr(0, equal);
        if (flag_options.contains(key)) {
            if (equal != std::string::npos) usage_error(key + " does not take a value");
            result.flags.insert(key);
            continue;
        }
        std::string value;
        if (equal != std::string::npos) value = token.substr(equal + 1);
        else {
            if (i + 1 >= argc || std::string(argv[i + 1]).starts_with("--")) usage_error("missing value for " + key);
            value = argv[++i];
        }
        if (!result.values.emplace(key, value).second) usage_error("option repeated: " + key);
    }
    return result;
}

void validate_command_options(const Arguments& args) {
    static const std::map<std::string, std::set<std::string>> command_values{
        {"init", {"--repo", "--storage-format"}},
        {"backup", {"--repo", "--source", "--workers", "--writer-workers", "--memory-mib", "--scheduler", "--max-workers",
                    "--zstd-level", "--failpoint", "--test-writer-delay-ms", "--test-reader-delay-ms", "--test-cpu-spin-ms"}},
        {"list", {"--repo"}},
        {"show", {"--repo", "--snapshot"}},
        {"restore", {"--repo", "--snapshot", "--target"}},
        {"verify", {"--repo", "--snapshot"}},
        {"stats", {"--repo"}},
        {"explain", {"--repo", "--run"}},
        {"benchmark", {"--dataset", "--workers", "--writer-workers", "--repeat", "--compare-schedulers", "--memory-mib", "--output", "--storage-format",
                        "--test-writer-delay-ms", "--test-reader-delay-ms", "--test-cpu-spin-ms"}},
    };
    static const std::map<std::string, std::set<std::string>> command_flags{
        {"init", {}},
        {"backup", {"--skip-unsupported", "--paranoid", "--trust-metadata"}},
        {"list", {}},
        {"show", {}},
        {"restore", {}},
        {"verify", {"--full"}},
        {"stats", {"--json"}},
        {"explain", {"--json"}},
        {"benchmark", {}},
    };
    const auto values = command_values.find(args.command);
    const auto flags = command_flags.find(args.command);
    if (values == command_values.end() || flags == command_flags.end()) return;

    for (const auto& [name, value] : args.values) {
        (void)value;
        if (!values->second.contains(name)) usage_error(name + " is not valid for command " + args.command);
    }
    for (const auto& name : args.flags) {
        if (name != "--help" && !flags->second.contains(name))
            usage_error(name + " is not valid for command " + args.command);
    }
}

std::string required(const Arguments& args, const std::string& name) {
    const auto it = args.values.find(name);
    if (it == args.values.end() || it->second.empty()) usage_error("missing required option " + name);
    return it->second;
}

std::string optional(const Arguments& args, const std::string& name, std::string fallback) {
    const auto it = args.values.find(name);
    return it == args.values.end() ? fallback : it->second;
}

std::size_t parse_size(const std::string& text, const std::string& label) {
    std::size_t value = 0;
    const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc{} || ptr != text.data() + text.size()) usage_error("invalid number for " + label + ": " + text);
    return value;
}

int parse_int(const std::string& text, const std::string& label) {
    int value = 0;
    const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc{} || ptr != text.data() + text.size()) usage_error("invalid integer for " + label + ": " + text);
    return value;
}

std::vector<std::size_t> parse_workers(const std::string& value) {
    std::vector<std::size_t> out;
    std::size_t begin = 0;
    while (begin <= value.size()) {
        const auto comma = value.find(',', begin);
        const auto end = comma == std::string::npos ? value.size() : comma;
        if (end == begin) usage_error("invalid empty worker count");
        out.push_back(parse_size(value.substr(begin, end - begin), "workers"));
        if (comma == std::string::npos) break;
        begin = comma + 1;
    }
    return out;
}

std::vector<std::string> parse_schedulers(const std::string& value) {
    std::vector<std::string> out;
    std::size_t begin = 0;
    while (begin <= value.size()) {
        const auto comma = value.find(',', begin);
        const auto end = comma == std::string::npos ? value.size() : comma;
        if (end == begin) usage_error("invalid empty scheduler");
        out.push_back(value.substr(begin, end - begin));
        if (comma == std::string::npos) break;
        begin = comma + 1;
    }
    return out;
}

std::string human_bytes(std::uint64_t bytes) {
    static constexpr const char* units[] = {"B", "KiB", "MiB", "GiB", "TiB"};
    double value = static_cast<double>(bytes);
    std::size_t unit = 0;
    while (value >= 1024.0 && unit + 1 < std::size(units)) { value /= 1024.0; ++unit; }
    std::ostringstream out;
    out << std::fixed << std::setprecision(unit == 0 ? 0 : 2) << value << ' ' << units[unit];
    return out.str();
}

std::string format_time_ns(std::int64_t value) {
    const std::time_t seconds = static_cast<std::time_t>(value / 1000000000ll);
    std::tm utc{};
    if (!::gmtime_r(&seconds, &utc)) return std::to_string(value) + " ns since epoch";
    char buffer[32]{};
    if (std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", &utc) == 0) return std::to_string(value) + " ns since epoch";
    return buffer;
}

void print_usage(std::ostream& out) {
    out << R"(Parallel Backup & Deduplication Engine (PBE)

Usage:
  pbe init --repo PATH
           [--storage-format files-v1]
  pbe backup --repo PATH --source PATH [--workers N] [--memory-mib N]
             [--scheduler fixed|adaptive] [--max-workers N]
             [--writer-workers 1]
             [--skip-unsupported] [--trust-metadata | --paranoid]
  pbe list --repo PATH
  pbe show --repo PATH --snapshot ID
  pbe restore --repo PATH --snapshot ID --target PATH
  pbe verify --repo PATH --snapshot ID [--full]
  pbe stats --repo PATH [--json]
  pbe explain --repo PATH --run RUN_ID [--json]
  pbe benchmark --dataset PATH [--workers 1,2,4,8] [--repeat N]
                [--compare-schedulers fixed,adaptive] [--memory-mib N]
                [--writer-workers 1] [--storage-format files-v1]
                [--output PATH] [--test-writer-delay-ms N]
                [--test-reader-delay-ms N] [--test-cpu-spin-ms N]

Options:
  --skip-unsupported  Skip special files and record no entry for them.
  --trust-metadata    Reuse same-path chunks when size/mode/mtime match.
  --paranoid          Always reread and hash source file contents.
  --full              Verify every referenced object's decompressed bytes and BLAKE3.
  --test-writer-delay-ms N  Benchmark-only writer throttle for diagnostics.
  --test-reader-delay-ms N  Benchmark-only input throttle for diagnostics.
  --test-cpu-spin-ms N      Test-only CPU work inside the admission gate.
  --failpoint NAME    Test-only abrupt exit at a named crash checkpoint.
  --help              Show this help.
)";
}

int error_code(const std::exception& error) {
    const std::string message = error.what();
    if (dynamic_cast<const std::invalid_argument*>(&error)) return 2;
    if (message.find("lock is already held") != std::string::npos) return 5;
    if (message.find("changed while") != std::string::npos || message.find("changed between") != std::string::npos) return 6;
    if (message.find("unsupported source entry") != std::string::npos) return 7;
    if (message.find("integrity") != std::string::npos || message.find("digest") != std::string::npos || message.find("object") != std::string::npos) return 4;
    if (message.find("No such file") != std::string::npos || message.find("Permission denied") != std::string::npos ||
        message.find("space left") != std::string::npos || message.find("read source") != std::string::npos ||
        message.find("write") != std::string::npos || message.find("sync") != std::string::npos) return 3;
    if (message.find("cancelled") != std::string::npos) return 8;
    if (message.find("must ") != std::string::npos || message.find("requires ") != std::string::npos ||
        message.find("not a PBE repository") != std::string::npos || message.find("unsupported repository") != std::string::npos ||
        message.find("missing required option") != std::string::npos || message.find("refusing to initialize") != std::string::npos ||
        message.find("snapshot not found") != std::string::npos) return 2;
    if (message.find("became shorter") != std::string::npos) return 6;
    if (message.find("disk I/O") != std::string::npos || message.find("unable to open database") != std::string::npos) return 3;
    return 9;
}

void print_backup(const pbe::BackupSummary& summary) {
    std::cout << "Snapshot:        " << summary.id << '\n'
              << "Source:          " << summary.source_root << '\n'
              << "Files:           " << summary.files << '\n'
              << "Directories:     " << summary.directories << '\n'
              << "Symlinks:        " << summary.symlinks << '\n'
              << "Logical Bytes:   " << human_bytes(summary.logical_bytes) << '\n'
              << "Read Bytes:      " << human_bytes(summary.read_bytes) << '\n'
              << "Unique Raw:      " << human_bytes(summary.unique_raw_bytes) << '\n'
              << "Reused Chunks:   " << summary.reused_chunks << '\n'
              << "New Stored:      " << human_bytes(summary.new_stored_bytes) << '\n'
              << "Elapsed:         " << std::fixed << std::setprecision(2) << summary.elapsed_seconds << " s\n"
              << "Scheduler:       " << summary.scheduler << '\n'
              << "Workers:         " << summary.workers << '\n'
              << "Writer Workers:  " << summary.writer_workers << '\n'
              << "Run ID:          " << summary.run_id << '\n'
              << "Status:          COMPLETE\n";
}

int run(int argc, char** argv) {
    const Arguments args = parse(argc, argv);
    if (args.flags.contains("--help") || args.command == "help") { print_usage(std::cout); return 0; }
    validate_command_options(args);

    if (args.command == "init") {
        pbe::init_repository(required(args, "--repo"), optional(args, "--storage-format", "files-v1"));
        std::cout << "Initialized repository at " << std::filesystem::absolute(required(args, "--repo")).string() << '\n';
        return 0;
    }
    if (args.command == "backup") {
        pbe::BackupOptions options;
        options.repo = required(args, "--repo"); options.source = required(args, "--source");
        options.workers = parse_size(optional(args, "--workers", "4"), "--workers");
        options.max_workers = parse_size(optional(args, "--max-workers", std::to_string(options.workers)), "--max-workers");
        options.writer_workers = parse_size(optional(args, "--writer-workers", "1"), "--writer-workers");
        if (options.writer_workers != 1)
            usage_error("--writer-workers must be 1 in this milestone");
        options.memory_mib = parse_size(optional(args, "--memory-mib", "256"), "--memory-mib");
        options.zstd_level = parse_int(optional(args, "--zstd-level", "3"), "--zstd-level");
        const auto scheduler = optional(args, "--scheduler", "fixed");
        if (scheduler != "fixed" && scheduler != "adaptive") usage_error("--scheduler must be fixed or adaptive");
        options.adaptive = scheduler == "adaptive";
        options.skip_unsupported = args.flags.contains("--skip-unsupported");
        options.paranoid = args.flags.contains("--paranoid");
        options.trust_metadata = args.flags.contains("--trust-metadata");
        options.failpoint = optional(args, "--failpoint", "");
        options.test_writer_delay_ms = parse_size(optional(args, "--test-writer-delay-ms", "0"), "--test-writer-delay-ms");
        options.test_reader_delay_ms = parse_size(optional(args, "--test-reader-delay-ms", "0"), "--test-reader-delay-ms");
        options.test_cpu_spin_ms = parse_size(optional(args, "--test-cpu-spin-ms", "0"), "--test-cpu-spin-ms");
        if (options.paranoid && options.trust_metadata) usage_error("--paranoid and --trust-metadata are mutually exclusive");
        print_backup(pbe::backup(options));
        return 0;
    }
    if (args.command == "list") {
        const auto snapshots = pbe::list_snapshots(required(args, "--repo"));
        if (snapshots.empty()) { std::cout << "No complete snapshots.\n"; return 0; }
        std::cout << std::left << std::setw(34) << "SNAPSHOT" << std::setw(9) << "FILES" << std::setw(11) << "CHUNKS" << std::setw(12) << "LOGICAL" << "SOURCE\n";
        for (const auto& s : snapshots)
            std::cout << std::left << std::setw(34) << s.id << std::setw(9) << s.files << std::setw(11) << s.chunks
                      << std::setw(12) << human_bytes(s.logical_bytes) << s.source_root << '\n';
        return 0;
    }
    if (args.command == "show") {
        const auto s = pbe::show_snapshot(required(args, "--repo"), required(args, "--snapshot"));
        std::cout << "Snapshot:      " << s.id << '\n'
                  << "Created:       " << format_time_ns(s.created_ns) << '\n'
                  << "Source:        " << s.source_root << '\n'
                  << "Files:         " << s.files << '\n'
                  << "Directories:   " << s.directories << '\n'
                  << "Symlinks:      " << s.symlinks << '\n'
                  << "Chunks:        " << s.chunks << '\n'
                  << "Logical bytes: " << human_bytes(s.logical_bytes) << '\n';
        return 0;
    }
    if (args.command == "restore") {
        pbe::restore_snapshot(pbe::RestoreOptions{required(args, "--repo"), required(args, "--target"), required(args, "--snapshot")});
        std::cout << "Restored snapshot " << required(args, "--snapshot") << " to " << required(args, "--target") << '\n';
        return 0;
    }
    if (args.command == "verify") {
        const auto s = pbe::verify_snapshot(required(args, "--repo"), required(args, "--snapshot"));
        std::cout << "Verified: COMPLETE snapshot objects and references are valid\n"
                  << "Files:    " << s.files << '\n'
                  << "Chunks:   " << s.chunks << '\n'
                  << "Bytes:    " << human_bytes(s.bytes) << '\n';
        return 0;
    }
    if (args.command == "stats") {
        const auto s = pbe::repository_stats(required(args, "--repo"));
        if (args.flags.contains("--json")) {
            std::cout << "{\"snapshots\":" << s.snapshots << ",\"objects\":" << s.object_count
                      << ",\"referenced_objects\":" << s.referenced_objects << ",\"logical_bytes\":" << s.logical_bytes
                      << ",\"physical_bytes\":" << s.physical_bytes << "}\n";
        } else {
            std::cout << "Complete snapshots: " << s.snapshots << '\n'
                      << "Objects:            " << s.object_count << '\n'
                      << "Referenced objects: " << s.referenced_objects << '\n'
                      << "Logical bytes:      " << human_bytes(s.logical_bytes) << '\n'
                      << "Physical bytes:     " << human_bytes(s.physical_bytes) << '\n';
        }
        return 0;
    }
    if (args.command == "explain") {
        std::cout << pbe::explain_run(required(args, "--repo"), required(args, "--run"), args.flags.contains("--json"));
        return 0;
    }
    if (args.command == "benchmark") {
        pbe::BenchmarkOptions options;
        options.dataset = required(args, "--dataset");
        options.writer_workers = parse_size(optional(args, "--writer-workers", "1"), "--writer-workers");
        if (options.writer_workers != 1)
            usage_error("--writer-workers must be 1 in this milestone");
        options.workers = parse_workers(optional(args, "--workers", "1,2,4,8"));
        options.repeat = parse_size(optional(args, "--repeat", "5"), "--repeat");
        options.memory_mib = parse_size(optional(args, "--memory-mib", "256"), "--memory-mib");
        options.storage_format = optional(args, "--storage-format", "files-v1");
        options.schedulers = parse_schedulers(optional(args, "--compare-schedulers", "fixed"));
        options.output = optional(args, "--output", "./pbe-benchmark");
        options.test_writer_delay_ms = parse_size(optional(args, "--test-writer-delay-ms", "0"), "--test-writer-delay-ms");
        options.test_reader_delay_ms = parse_size(optional(args, "--test-reader-delay-ms", "0"), "--test-reader-delay-ms");
        options.test_cpu_spin_ms = parse_size(optional(args, "--test-cpu-spin-ms", "0"), "--test-cpu-spin-ms");
        std::cout << pbe::run_benchmark(options) << '\n';
        return 0;
    }
    usage_error("unknown command: " + args.command);
}

void on_sigint(int) { pbe::note_interrupt(); }
}

int main(int argc, char** argv) {
    std::signal(SIGINT, on_sigint);
    try { return run(argc, argv); }
    catch (const std::exception& error) {
        std::cerr << "pbe: " << error.what() << '\n';
        const int code = error_code(error);
        if (code == 2) { std::cerr << "Run 'pbe --help' for usage.\n"; return 2; }
        return code;
    }
}
