#pragma once

#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <string>

namespace pbe::detail {
struct WorkerError;

// A source has no input queue; it produces tasks for downstream stages.
// run executes on the caller thread; request_stop can wake source-owned waits during cancellation.
struct PipelineSource {
    std::string name;
    std::function<void()> run;
    std::function<void()> request_stop = {};
};

// Stage configuration: run is a worker's full loop, usually popping tasks until input closes.
// close_input shuts the stage's input and wakes workers waiting for tasks.
struct PipelineStage {
    std::string name;
    std::size_t workers = 1;
    std::function<void(std::size_t)> run;
    std::function<void()> close_input;
};

// Observers run beside stages without forwarding data, for example a periodic metrics monitor.
struct PipelineObserver {
    std::string name;
    std::function<void()> run;
    std::function<void()> request_stop;
};

// Manage startup, draining, and cancellation without depending on SQLite or backup formats.
//
// Normal use: set_source / add_stage / add_observer -> run.
// Phased lifecycle control: start -> run_source -> finish.
// Register stages in data-flow order; start downstream first and drain upstream first.
// Run exactly one source after workers start; stop observers after workers finish.
//
// The caller serializes lifecycle methods; workers must synchronize their own shared state.
// Stop callbacks must be repeatable, concurrent-safe, nonthrowing, nonblocking, and wake waiters.
// Stop callbacks must also handle preparation failure and sources not yet started or already done.
// Sources must check cancellation, react to closed inputs, or provide request_stop for private waits.
// Queues, errors, and other resources captured by callbacks must outlive the runtime.
class PipelineRuntime {
public:
    explicit PipelineRuntime(WorkerError& errors, std::function<void()> cancel_resources = {});
    ~PipelineRuntime();
    PipelineRuntime(const PipelineRuntime&) = delete;
    PipelineRuntime& operator=(const PipelineRuntime&) = delete;

    // Sources, stages, and observers share one namespace; registration closes when startup begins.
    void set_source(PipelineSource source);
    void add_stage(PipelineStage stage);
    void add_observer(PipelineObserver observer);

    // Run once end to end; drain on success or cancel and join before rethrowing the first error.
    void run(const std::function<void()>& prepare = {});

    // Prepare before creating threads; start returns before the source begins reading.
    void start(const std::function<void()>& prepare = {});
    void run_source();

    // Call after the source finishes; drain accepted tasks in stage order.
    void finish();
    void cancel_and_join(std::exception_ptr failure = {});

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace pbe::detail
