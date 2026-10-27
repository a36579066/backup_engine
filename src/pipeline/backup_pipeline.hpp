#pragma once

#include <atomic>
#include <exception>
#include <memory>

namespace pbe::detail {
class BackupSession;
class PipelineRuntime;
struct PipelineContext;

// Wire backup-specific queues and work functions into the generic runtime.
// Start reading at configure_stages(): it defines each stage's task and worker count.
class BackupPipeline {
public:
    explicit BackupPipeline(BackupSession& session);
    ~BackupPipeline();
    BackupPipeline(const BackupPipeline&) = delete;
    BackupPipeline& operator=(const BackupPipeline&) = delete;
    // Normal entry point: prepare, start workers, run the source, drain, and join.
    void run();

    // Use phased lifecycle methods in order, or cancel_and_join to stop early.
    void start();
    void run_source();
    void finish();
    void cancel_and_join(std::exception_ptr failure);
    const PipelineContext& context() const;

private:
    void configure_stages();
    std::unique_ptr<PipelineContext> context_;
    std::atomic<bool> monitor_stop_{false};

    // Reverse member destruction joins the runtime before releasing context_.
    std::unique_ptr<PipelineRuntime> runtime_;
};
} // namespace pbe::detail
