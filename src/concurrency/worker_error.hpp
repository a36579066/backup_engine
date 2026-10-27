#pragma once

#include <atomic>
#include <cstdint>
#include <exception>
#include <mutex>
#include <stdexcept>

namespace pbe::detail {
// Preserve the first worker error so later cleanup failures cannot hide the original cause.
// cancelled is the shared stop flag; the exception itself is protected by a mutex.
struct WorkerError {
    std::mutex mutex;
    std::exception_ptr exception;
    std::atomic<bool> cancelled{false};
    std::atomic<std::uint64_t> error_count{0};

    void set(std::exception_ptr value) {
        bool first = false;
        {
            std::lock_guard lock(mutex);
            if (!exception) {
                exception = value;
                first = true;
            }
        }
        if (first)
            error_count.fetch_add(1, std::memory_order_relaxed);
        cancelled.store(true, std::memory_order_relaxed);
    }

    void rethrow() {
        std::lock_guard lock(mutex);
        if (exception)
            std::rethrow_exception(exception);
        if (cancelled.load())
            throw std::runtime_error("backup cancelled");
    }
};

} // namespace pbe::detail
