#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <utility>

namespace pbe::detail {
class MemoryBudget {
    using Clock = std::chrono::steady_clock;
public:
    class Credit {
    public:
        Credit() = default;
        Credit(MemoryBudget* owner, std::size_t amount) : owner_(owner), amount_(amount) {}
        Credit(Credit&& other) noexcept : owner_(std::exchange(other.owner_, nullptr)), amount_(other.amount_) {}
        Credit& operator=(Credit&& other) noexcept {
            if (this != &other) { release(); owner_ = std::exchange(other.owner_, nullptr); amount_ = other.amount_; }
            return *this;
        }
        ~Credit() { release(); }
        Credit(const Credit&) = delete;
        Credit& operator=(const Credit&) = delete;
        void release() { if (owner_) { owner_->release(amount_); owner_ = nullptr; } }
    private:
        MemoryBudget* owner_ = nullptr;
        std::size_t amount_ = 0;
    };

    explicit MemoryBudget(std::size_t bytes) : limit_(bytes) {}
    Credit acquire(std::size_t amount, const std::atomic<bool>& cancelled) {
        if (amount > limit_) throw std::runtime_error("memory budget is too small for one chunk and its compression buffer");
        const auto start = Clock::now();
        std::unique_lock lock(mutex_);
        while (used_ + amount > limit_ && !cancelled.load()) cv_.wait_for(lock, std::chrono::milliseconds(50));
        wait_ns_.fetch_add(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count(), std::memory_order_relaxed);
        if (cancelled.load()) throw std::runtime_error("backup cancelled while waiting for memory credits");
        used_ += amount;
        peak_ = std::max(peak_, used_);
        return Credit(this, amount);
    }
    std::size_t current() const { std::lock_guard lock(mutex_); return used_; }
    std::size_t peak() const { std::lock_guard lock(mutex_); return peak_; }
    std::size_t limit() const { return limit_; }
    std::uint64_t wait_ns() const { return wait_ns_.load(std::memory_order_relaxed); }
private:
    void release(std::size_t amount) {
        std::lock_guard lock(mutex_);
        used_ -= amount;
        cv_.notify_all();
    }
    const std::size_t limit_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::size_t used_ = 0, peak_ = 0;
    std::atomic<std::uint64_t> wait_ns_{0};
};
} // namespace pbe::detail
