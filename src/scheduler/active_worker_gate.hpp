#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>

namespace pbe::detail {
class ActiveWorkerGate {
    using Clock = std::chrono::steady_clock;
public:
    ActiveWorkerGate(std::size_t maximum, std::size_t initial) : maximum_(maximum), limit_(std::clamp(initial, std::size_t(1), maximum)) {}
    bool acquire(const std::atomic<bool>& cancelled) {
        const auto start = Clock::now();
        std::unique_lock lock(mutex_);
        while (!closed_ && active_ >= limit_ && !cancelled.load()) cv_.wait_for(lock, std::chrono::milliseconds(50));
        wait_ns_.fetch_add(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count(), std::memory_order_relaxed);
        if (closed_ || cancelled.load()) return false;
        ++active_;
        return true;
    }
    void release() {
        std::lock_guard lock(mutex_);
        if (active_ > 0) --active_;
        cv_.notify_all();
    }
    void set_limit(std::size_t value) {
        std::lock_guard lock(mutex_);
        limit_ = std::clamp(value, std::size_t(1), maximum_);
        cv_.notify_all();
    }
    std::size_t limit() const { std::lock_guard lock(mutex_); return limit_; }
    std::uint64_t wait_ns() const { return wait_ns_.load(std::memory_order_relaxed); }
    void close() { std::lock_guard lock(mutex_); closed_ = true; cv_.notify_all(); }
private:
    const std::size_t maximum_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::size_t limit_, active_ = 0;
    bool closed_ = false;
    std::atomic<std::uint64_t> wait_ns_{0};
};
} // namespace pbe::detail
