#include "pipeline/00_framework/pipeline_runtime.hpp"
#include "concurrency/worker_error.hpp"
#include "concurrency/worker_pool.hpp"

#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace pbe::detail {
struct PipelineRuntime::Impl {
    // The caller owns lifecycle state; workers report failures through shared WorkerError.
    enum class State {
        ready,
        starting,
        running,
        feeding,
        source_complete,
        joining,
        joined
    };

    struct StageGroup {
        PipelineStage definition;
        FixedWorkerPool threads;
    };

    struct Observer {
        PipelineObserver definition;
        std::thread thread;
    };

    WorkerError& errors;
    std::function<void()> cancel_resources;
    std::optional<PipelineSource> source;
    std::vector<std::unique_ptr<StageGroup>> stages;
    std::vector<std::unique_ptr<Observer>> observers;
    std::once_flag cancellation;
    State state = State::ready;

    Impl(WorkerError& shared_errors, std::function<void()> cancel)
        : errors(shared_errors),
          cancel_resources(std::move(cancel)) {
    }

    void require_ready() const {
        if (state != State::ready)
            throw std::logic_error("pipeline configuration is already closed");
    }

    void require_unique_name(const std::string& name) const {
        if (name.empty())
            throw std::invalid_argument("pipeline component name must not be empty");
        if (source && source->name == name)
            throw std::invalid_argument("duplicate pipeline component name: " + name);
        for (const auto& stage : stages)
            if (stage->definition.name == name)
                throw std::invalid_argument("duplicate pipeline component name: " + name);
        for (const auto& observer : observers)
            if (observer->definition.name == name)
                throw std::invalid_argument("duplicate pipeline component name: " + name);
    }

    void require_complete_configuration() const {
        require_ready();
        if (!source)
            throw std::logic_error("pipeline needs a registered source");
        if (stages.empty())
            throw std::logic_error("pipeline has no worker stages");
    }

    void cleanup(const std::function<void()>& action) noexcept {
        try {
            if (action)
                action();
        } catch (...) {
            errors.set(std::current_exception());
        }
    }

    void request_cancel(std::exception_ptr failure) noexcept {
        if (failure)
            errors.set(failure);
        else
            errors.cancelled.store(true, std::memory_order_relaxed);

        // Run cancellation once even if several workers fail, and wake all waiting stages.
        std::call_once(cancellation, [this] {
            if (source)
                cleanup(source->request_stop);
            for (const auto& stage : stages)
                cleanup(stage->definition.close_input);
            for (const auto& observer : observers)
                cleanup(observer->definition.request_stop);
            cleanup(cancel_resources);
        });
    }

    template <class Work>
    void execute(Work&& work) noexcept {
        // Exceptions must not escape a thread; retain the cause for the caller to rethrow after joining.
        try {
            work();
        } catch (...) {
            request_cancel(std::current_exception());
        }
    }

    void start_worker_stages() {
        // Start downstream consumers before upstream producers can submit work.
        for (auto it = stages.rbegin(); it != stages.rend(); ++it) {
            auto* stage = it->get();
            stage->threads.start(stage->definition.workers, [this, stage](std::size_t index) {
                execute([&] {
                    stage->definition.run(index);
                });
            });
            errors.rethrow();
        }
    }

    void start_observers() {
        for (const auto& entry : observers) {
            auto* observer = entry.get();
            observer->thread = std::thread([this, observer] {
                execute(observer->definition.run);
            });
        }
    }

    void drain_worker_stages() {
        // Close each downstream input only after its upstream workers have fully exited.
        for (const auto& stage : stages) {
            try {
                stage->definition.close_input();
            } catch (...) {
                request_cancel(std::current_exception());
            }
            stage->threads.join();
        }
    }

    void stop_and_join_observers() {
        for (const auto& observer : observers) {
            try {
                observer->definition.request_stop();
            } catch (...) {
                request_cancel(std::current_exception());
            }
            if (observer->thread.joinable())
                observer->thread.join();
        }
    }

    void join_all() {
        for (const auto& stage : stages)
            stage->threads.join();
        for (const auto& observer : observers)
            if (observer->thread.joinable())
                observer->thread.join();
        state = State::joined;
    }

    void cancel_and_join(std::exception_ptr failure) {
        if (state == State::joined)
            return;
        request_cancel(failure);
        state = State::joining;
        join_all();
    }
};

PipelineRuntime::PipelineRuntime(WorkerError& errors, std::function<void()> cancel_resources)
    : impl_(std::make_unique<Impl>(errors, std::move(cancel_resources))) {
}

PipelineRuntime::~PipelineRuntime() {
    // Scope exit must join workers even when an exception bypasses finish().
    impl_->cancel_and_join({});
}

void PipelineRuntime::set_source(PipelineSource source) {
    impl_->require_ready();
    if (impl_->source)
        throw std::logic_error("pipeline source is already registered");
    impl_->require_unique_name(source.name);
    if (!source.run)
        throw std::invalid_argument("pipeline source needs a task");
    impl_->source = std::move(source);
}

void PipelineRuntime::add_stage(PipelineStage stage) {
    impl_->require_ready();
    impl_->require_unique_name(stage.name);
    if (stage.workers == 0 || !stage.run || !stage.close_input)
        throw std::invalid_argument("pipeline stage needs workers, a task and an input closer");
    auto group = std::make_unique<Impl::StageGroup>();
    group->definition = std::move(stage);
    impl_->stages.push_back(std::move(group));
}

void PipelineRuntime::add_observer(PipelineObserver observer) {
    impl_->require_ready();
    impl_->require_unique_name(observer.name);
    if (!observer.run || !observer.request_stop)
        throw std::invalid_argument("pipeline observer needs a task and a stop callback");
    auto entry = std::make_unique<Impl::Observer>();
    entry->definition = std::move(observer);
    impl_->observers.push_back(std::move(entry));
}

void PipelineRuntime::run(const std::function<void()>& prepare) {
    start(prepare);
    try {
        run_source();
        finish();
    } catch (...) {
        // Join every worker before rethrowing so captured resources are no longer in use.
        impl_->cancel_and_join(std::current_exception());
        impl_->errors.rethrow();
        throw;
    }
}

void PipelineRuntime::start(const std::function<void()>& prepare) {
    impl_->require_complete_configuration();
    impl_->state = Impl::State::starting;
    try {
        impl_->errors.rethrow();
        if (prepare)
            prepare();

        impl_->start_worker_stages();
        impl_->start_observers();
        impl_->errors.rethrow();
        impl_->state = Impl::State::running;
    } catch (...) {
        const auto failure = std::current_exception();

        // Partial startup also needs cancellation: wake and join every thread already created.
        impl_->cancel_and_join(failure);
        impl_->errors.rethrow();
        throw;
    }
}

void PipelineRuntime::run_source() {
    if (impl_->state != Impl::State::running)
        throw std::logic_error(
            "registered source requires a running pipeline and can run only once");
    impl_->state = Impl::State::feeding;
    try {
        impl_->errors.rethrow();

        // Run the source on the caller thread while background workers consume the queues.
        impl_->source->run();
        impl_->errors.rethrow();
        impl_->state = Impl::State::source_complete;
    } catch (...) {
        impl_->request_cancel(std::current_exception());
        impl_->errors.rethrow();
        throw;
    }
}

void PipelineRuntime::finish() {
    if (impl_->state == Impl::State::joined) {
        impl_->errors.rethrow();
        return;
    }
    if (impl_->state != Impl::State::source_complete)
        throw std::logic_error("pipeline source must finish before draining worker stages");
    impl_->state = Impl::State::joining;
    if (impl_->errors.cancelled.load(std::memory_order_relaxed))
        impl_->request_cancel({});

    impl_->drain_worker_stages();
    impl_->stop_and_join_observers();
    impl_->state = Impl::State::joined;
    impl_->errors.rethrow();
}

void PipelineRuntime::cancel_and_join(std::exception_ptr failure) {
    impl_->cancel_and_join(failure);
}
} // namespace pbe::detail
