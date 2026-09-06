#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace pbe::detail {
template<class Value>
struct ClaimState {
    std::mutex mutex;
    std::condition_variable cv;
    bool ready = false;
    Value value{};
    std::exception_ptr error;
};

template<class Value>
class ClaimRegistry {
public:
    using State = ClaimState<Value>;

    std::pair<std::shared_ptr<State>, bool> claim(const std::string& id) {
        std::lock_guard lock(mutex_);
        auto it = states_.find(id);
        if (it != states_.end()) return {it->second, false};
        auto state = std::make_shared<State>();
        states_.emplace(id, state);
        return {state, true};
    }
    void complete(const std::string& id, const std::shared_ptr<State>& state, const Value& value) {
        (void)id;
        { std::lock_guard lock(state->mutex); state->value = value; state->ready = true; }
        state->cv.notify_all();
    }
    void fail(const std::string& id, const std::shared_ptr<State>& state, std::exception_ptr error) {
        { std::lock_guard lock(state->mutex); state->error = error; state->ready = true; }
        state->cv.notify_all();
        erase_if_current(id, state);
    }
    // Keep a completed claim discoverable until the coordinator commits its
    // object row. Otherwise a later duplicate can miss SQLite and publish the
    // same content a second time while its first row is still pending.
    void release(const std::string& id) {
        std::shared_ptr<State> state;
        {
            std::lock_guard lock(mutex_);
            const auto it = states_.find(id);
            if (it == states_.end()) return;
            state = it->second;
        }
        {
            std::lock_guard lock(state->mutex);
            if (!state->ready || state->error) return;
        }
        erase_if_current(id, state);
    }
    Value wait(const std::shared_ptr<State>& state, const std::atomic<bool>& cancelled) {
        std::unique_lock lock(state->mutex);
        while (!state->ready && !cancelled.load()) state->cv.wait_for(lock, std::chrono::milliseconds(50));
        if (!state->ready) throw std::runtime_error("backup cancelled while waiting for a duplicate chunk");
        if (state->error) std::rethrow_exception(state->error);
        return state->value;
    }
    std::size_t size() const { std::lock_guard lock(mutex_); return states_.size(); }
private:
    void erase_if_current(const std::string& id, const std::shared_ptr<State>& state) {
        std::lock_guard lock(mutex_);
        const auto it = states_.find(id);
        if (it != states_.end() && it->second == state) states_.erase(it);
    }
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<State>> states_;
};
} // namespace pbe::detail
