#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace pbe::detail {
// Shared digest result: one owner produces it; duplicate tasks subscribe to completion.
template <class Value>
struct ClaimState {
    std::mutex mutex;
    std::condition_variable cv;
    bool ready = false;
    Value value{};
    std::exception_ptr error;
    std::vector<std::function<void(const Value*, std::exception_ptr)>> continuations;
};

template <class Value>
class ClaimRegistry {
public:
    using State = ClaimState<Value>;

    // Return {state, is_new_owner}; lookup and creation occur under the same lock.
    std::pair<std::shared_ptr<State>, bool> claim(const std::string& id) {
        std::lock_guard lock(mutex_);
        auto it = states_.find(id);
        if (it != states_.end())
            return {it->second, false};
        auto state = std::make_shared<State>();
        states_.emplace(id, state);
        return {state, true};
    }

    void complete(const std::string& id, const std::shared_ptr<State>& state, const Value& value) {
        (void)id;
        std::vector<std::function<void(const Value*, std::exception_ptr)>> continuations;
        {
            std::lock_guard lock(state->mutex);
            if (state->ready)
                return;
            state->value = value;
            state->ready = true;
            continuations.swap(state->continuations);
        }

        // Invoke callbacks after unlocking so callbacks can safely reenter the registry.
        state->cv.notify_all();
        for (auto& continuation : continuations) {
            try {
                continuation(&state->value, {});
            } catch (...) {
            }
        }
    }

    void fail(const std::string& id, const std::shared_ptr<State>& state,
              std::exception_ptr error) {
        std::vector<std::function<void(const Value*, std::exception_ptr)>> continuations;
        {
            std::lock_guard lock(state->mutex);
            if (state->ready)
                return;
            state->error = error;
            state->ready = true;
            continuations.swap(state->continuations);
        }
        state->cv.notify_all();
        for (auto& continuation : continuations) {
            try {
                continuation(nullptr, error);
            } catch (...) {
            }
        }
        erase_if_current(id, state);
    }

    void when_ready(const std::shared_ptr<State>& state,
                    std::function<void(const Value*, std::exception_ptr)> continuation) {
        std::exception_ptr error;
        {
            std::lock_guard lock(state->mutex);
            if (!state->ready) {
                state->continuations.push_back(std::move(continuation));
                return;
            }
            error = state->error;
        }
        continuation(error ? nullptr : &state->value, error);
    }

    // Keep completed claims until the Metadata Writer commits their object index entries.
    // Earlier removal could let duplicates miss uncommitted SQLite rows and write the content again.
    void release(const std::string& id) {
        std::shared_ptr<State> state;
        {
            std::lock_guard lock(mutex_);
            const auto it = states_.find(id);
            if (it == states_.end())
                return;
            state = it->second;
        }
        {
            std::lock_guard lock(state->mutex);
            if (!state->ready || state->error)
                return;
        }
        erase_if_current(id, state);
    }

    Value wait(const std::shared_ptr<State>& state, const std::atomic<bool>& cancelled) {
        std::unique_lock lock(state->mutex);
        while (!state->ready && !cancelled.load())
            state->cv.wait_for(lock, std::chrono::milliseconds(50));
        if (!state->ready)
            throw std::runtime_error("backup cancelled while waiting for a duplicate chunk");
        if (state->error)
            std::rethrow_exception(state->error);
        return state->value;
    }

    std::size_t size() const {
        std::lock_guard lock(mutex_);
        return states_.size();
    }

private:
    void erase_if_current(const std::string& id, const std::shared_ptr<State>& state) {
        // Match the shared state before erasing to preserve a newer claim with the same ID.
        std::lock_guard lock(mutex_);
        const auto it = states_.find(id);
        if (it != states_.end() && it->second == state)
            states_.erase(it);
    }

    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<State>> states_;
};
} // namespace pbe::detail
