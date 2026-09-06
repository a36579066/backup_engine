#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <optional>
#include <queue>
#include <utility>

namespace pbe {
template<class T>
class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t capacity) : capacity_(capacity ? capacity : 1) {}
    BoundedQueue(const BoundedQueue&) = delete;
    BoundedQueue& operator=(const BoundedQueue&) = delete;

    bool push(T item, const std::atomic<bool>* cancelled = nullptr) {
        std::unique_lock lock(mutex_);
        while (!closed_ && queue_.size() >= capacity_ && !(cancelled && cancelled->load())) {
            not_full_.wait_for(lock, std::chrono::milliseconds(50));
        }
        if (closed_ || (cancelled && cancelled->load())) return false;
        queue_.push(std::move(item));
        not_empty_.notify_one();
        return true;
    }

    std::optional<T> pop(const std::atomic<bool>* cancelled = nullptr) {
        std::unique_lock lock(mutex_);
        while (queue_.empty() && !closed_ && !(cancelled && cancelled->load())) {
            not_empty_.wait_for(lock, std::chrono::milliseconds(50));
        }
        if (queue_.empty()) return std::nullopt;
        T item = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return item;
    }

    template<class Clock, class Duration>
    std::optional<T> pop_until(const std::chrono::time_point<Clock, Duration>& deadline) {
        std::unique_lock lock(mutex_);
        not_empty_.wait_until(lock, deadline, [&] { return closed_ || !queue_.empty(); });
        if (queue_.empty()) return std::nullopt;
        T item = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return item;
    }

    void close() {
        std::lock_guard lock(mutex_);
        closed_ = true;
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    std::size_t size() const {
        std::lock_guard lock(mutex_);
        return queue_.size();
    }
    std::size_t capacity() const noexcept { return capacity_; }

private:
    const std::size_t capacity_;
    mutable std::mutex mutex_;
    std::condition_variable not_empty_, not_full_;
    std::queue<T> queue_;
    bool closed_ = false;
};
} // namespace pbe
