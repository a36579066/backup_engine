#include "pipeline/shared/stage_support.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <thread>

namespace pbe::detail {
using Clock = PipelineClock;

std::uint64_t elapsed_ns(Clock::time_point start) {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());
}

std::string error_text(std::string_view operation) {
    return std::string(operation) + ": " + std::strerror(errno);
}

std::uint64_t get_le(const std::uint8_t* in, std::size_t bytes) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < bytes; ++i)
        value |= std::uint64_t(in[i]) << (8 * i);
    return value;
}

bool sleep_cancellable(std::size_t milliseconds, const std::atomic<bool>& cancelled) {
    // Split test delays into at most 50 ms intervals so cancellation is observed promptly.
    std::size_t remaining = milliseconds;
    while (remaining && !cancelled.load(std::memory_order_relaxed)) {
        const auto step = std::min<std::size_t>(remaining, 50);
        std::this_thread::sleep_for(std::chrono::milliseconds(step));
        remaining -= step;
    }
    return !cancelled.load(std::memory_order_relaxed);
}

std::atomic<std::uint64_t> g_test_cpu_sink{0};

// Retain injected compute results to prevent optimization; normal operation uses zero milliseconds.
bool spin_cpu_cancellable(std::size_t milliseconds, const std::atomic<bool>& cancelled,
                          std::uint64_t seed) {
    if (milliseconds == 0)
        return !cancelled.load(std::memory_order_relaxed);
    const auto until = Clock::now() + std::chrono::milliseconds(milliseconds);
    std::uint64_t value = seed | 1u;
    while (Clock::now() < until && !cancelled.load(std::memory_order_relaxed)) {
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        value += 0x9e3779b97f4a7c15ull;
    }
    g_test_cpu_sink.fetch_xor(value, std::memory_order_relaxed);
    return !cancelled.load(std::memory_order_relaxed);
}

} // namespace pbe::detail
