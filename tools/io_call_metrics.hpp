#pragma once

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <libproc.h>
#include <mutex>
#include <ostream>
#include <stdexcept>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>
#include <vector>

namespace pbe::profiling {
using Clock = std::chrono::steady_clock;

inline std::uint64_t thread_cpu_ns() {
    timespec time{};
    if (::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &time) != 0)
        throw std::runtime_error("thread CPU clock is unavailable");
    return static_cast<std::uint64_t>(time.tv_sec) * 1000000000ull + time.tv_nsec;
}

// Per-call wall and thread CPU time; the difference includes waiting/descheduling, not SSD busy time.
struct CallSample {
    std::uint64_t wall_ns;
    std::uint64_t cpu_ns;
    std::int64_t result;
};

class IoCalls {
public:
    IoCalls() {
        samples_.reserve(4096);
    }

    void add(CallSample sample) {
        std::lock_guard lock(mutex_);
        samples_.push_back(sample);
    }

    bool empty() const {
        std::lock_guard lock(mutex_);
        return samples_.empty();
    }

    void json(std::ostream& out) const {
        std::lock_guard lock(mutex_);
        std::uint64_t wall = 0, cpu = 0, bytes = 0, errors = 0;
        std::vector<std::uint64_t> durations;
        for (const auto& sample : samples_) {
            wall += sample.wall_ns;
            cpu += sample.cpu_ns;
            if (sample.result > 0)
                bytes += sample.result;
            if (sample.result < 0)
                ++errors;
            durations.push_back(sample.wall_ns);
        }
        std::sort(durations.begin(), durations.end());
        const auto percentile = [&](std::size_t percent) {
            return durations.empty() ? 0.0
                                     : durations[(durations.size() - 1) * percent / 100] / 1e6;
        };
        out << "{\"count\":" << samples_.size() << ",\"bytes\":" << bytes
            << ",\"errors\":" << errors << ",\"wall_seconds\":" << wall / 1e9
            << ",\"thread_cpu_seconds\":" << cpu / 1e9
            << ",\"off_cpu_seconds\":" << (wall > cpu ? wall - cpu : 0) / 1e9
            << ",\"p50_ms\":" << percentile(50) << ",\"p95_ms\":" << percentile(95)
            << ",\"max_ms\":" << percentile(100) << ",\"samples_ns\":[";
        for (std::size_t i = 0; i < samples_.size(); ++i) {
            if (i)
                out << ',';
            const auto& sample = samples_[i];
            out << '[' << sample.wall_ns << ',' << sample.cpu_ns << ',' << sample.result << ']';
        }
        out << "]}";
    }

private:
    mutable std::mutex mutex_;
    std::vector<CallSample> samples_;
};

template <class Operation>
auto measure(IoCalls& calls, Operation&& operation) {
    const auto wall_begin = Clock::now();
    const auto cpu_begin = thread_cpu_ns();
    const auto result = operation();
    const int saved_errno = errno;
    const auto cpu = thread_cpu_ns() - cpu_begin;
    const auto wall =
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - wall_begin).count();
    calls.add({static_cast<std::uint64_t>(wall), cpu, static_cast<std::int64_t>(result)});
    errno = saved_errno;
    return result;
}

// Own-process OS I/O accounting, not whole-device utilization or physical NAND traffic.
struct ProcessIo {
    rusage_info_v4 info{};
    bool available = false;
    int error = 0;

    static ProcessIo read() {
        ProcessIo value;
        value.available = ::proc_pid_rusage(::getpid(), RUSAGE_INFO_V4,
                                            reinterpret_cast<rusage_info_t*>(&value.info)) == 0;
        value.error = value.available ? 0 : errno;
        return value;
    }
};

inline void process_io_json(std::ostream& out, const ProcessIo& before, const ProcessIo& after) {
    const bool valid = before.available && after.available;
    out << "{\"available\":" << (valid ? "true" : "false")
        << ",\"error\":" << (before.error ? before.error : after.error);
    if (valid)
        out << ",\"read_bytes\":"
            << after.info.ri_diskio_bytesread - before.info.ri_diskio_bytesread
            << ",\"written_bytes\":"
            << after.info.ri_diskio_byteswritten - before.info.ri_diskio_byteswritten;
    out << '}';
}
} // namespace pbe::profiling
