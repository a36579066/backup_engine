#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace pbe::detail {
using PipelineClock = std::chrono::steady_clock;

// Shared timing, test-load, and byte-decoding helpers; no queue or thread lifecycle ownership.
std::uint64_t elapsed_ns(PipelineClock::time_point start);
bool sleep_cancellable(std::size_t milliseconds, const std::atomic<bool>& cancelled);
bool spin_cpu_cancellable(std::size_t milliseconds, const std::atomic<bool>& cancelled,
                          std::uint64_t seed);
std::uint64_t get_le(const std::uint8_t* in, std::size_t bytes);
std::string error_text(std::string_view operation);
} // namespace pbe::detail
