#pragma once

#include <cstddef>
#include <stdexcept>
#include <thread>
#include <vector>

namespace pbe::detail {
// Own a fixed number of threads; the caller supplies each worker's task loop.
// PipelineRuntime requests shutdown by closing inputs before joining these threads.
class FixedWorkerPool {
public:
    FixedWorkerPool() = default;
    FixedWorkerPool(const FixedWorkerPool&) = delete;
    FixedWorkerPool& operator=(const FixedWorkerPool&) = delete;

    template <class Worker>
    void start(std::size_t count, Worker&& worker) {
        if (!workers_.empty())
            throw std::logic_error("fixed worker pool can only be started once");
        workers_.reserve(count);
        for (std::size_t i = 0; i < count; ++i)
            workers_.emplace_back(worker, i);
    }

    void join() {
        for (auto& worker : workers_)
            if (worker.joinable())
                worker.join();
    }

    std::size_t size() const noexcept {
        return workers_.size();
    }

private:
    // join only waits for threads; it does not request their termination.
    std::vector<std::thread> workers_;
};
} // namespace pbe::detail
