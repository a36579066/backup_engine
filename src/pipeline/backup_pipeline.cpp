#include "pipeline/backup_pipeline.hpp"
#include "pipeline/00_framework/pipeline_runtime.hpp"
#include "pipeline/01_reader/reader_stage.hpp"
#include "pipeline/02_cpu/cpu_stage.hpp"
#include "pipeline/03_storage/storage_stage.hpp"
#include "pipeline/04_metadata/metadata_writer.hpp"
#include "pipeline/shared/pipeline_context.hpp"
#include "diagnostics/pipeline_monitor.hpp"

#include <stdexcept>

namespace pbe::detail {
namespace {
void check_interrupt() {
    if (interrupt_requested())
        throw std::runtime_error("backup cancelled by interrupt");
}
} // namespace

BackupPipeline::BackupPipeline(BackupSession& session)
    : context_(std::make_unique<PipelineContext>(session)),
      runtime_(std::make_unique<PipelineRuntime>(context_->errors, [this] {
          context_->gate.close();
      })) {
    configure_stages();
}

BackupPipeline::~BackupPipeline() = default;

void BackupPipeline::configure_stages() {
    // Register in 01 -> 04 data-flow order; runtime starts 04 -> 03 -> 02, then the monitor.
    // Run the source on the caller thread without an input queue, overlapping downstream workers.
    runtime_->set_source({
        .name = "01_reader",
        .run =
            [this] {
                read_source_files(*context_);
            },
    });

    runtime_->add_stage({
        .name = "02_cpu",
        .workers = context_->workers_count,
        .run =
            [this](std::size_t index) {
                run_chunk_worker(*context_, index);
            },
        .close_input =
            [this] {
                context_->input_queue.close();
            },
    });

    // CPU workers enqueue compressed objects; Storage workers consume object_queue.
    runtime_->add_stage({
        .name = "03_storage",
        .workers = context_->options.writer_workers,
        .run =
            [this](std::size_t) {
                if (context_->packed_storage)
                    run_pack_object_writer(*context_);
                else
                    run_loose_object_writer(*context_);
            },
        .close_input =
            [this] {
                context_->object_queue.close();
            },
    });

    // One owner writes SQLite, waiting for each required durable object result before committing.
    runtime_->add_stage({
        .name = "04_metadata",
        .workers = 1,
        .run =
            [this](std::size_t) {
                run_metadata_writer(*context_);
            },
        .close_input =
            [this] {
                context_->metadata_queue.close();
            },
    });

    // The monitor observes and adjusts CPU admission without becoming a data-processing stage.
    runtime_->add_observer({
        .name = "monitor",
        .run =
            [this] {
                run_pipeline_monitor(*context_, monitor_stop_);
            },
        .request_stop =
            [this] {
                monitor_stop_.store(true, std::memory_order_relaxed);
            },
    });
}

void BackupPipeline::run() {
    runtime_->run([this] {
        prepare_cpu_workers(*context_);
    });
    check_interrupt();
}

void BackupPipeline::start() {
    runtime_->start([this] {
        prepare_cpu_workers(*context_);
    });
}

void BackupPipeline::run_source() {
    runtime_->run_source();
}

void BackupPipeline::finish() {
    runtime_->finish();
    check_interrupt();
}

void BackupPipeline::cancel_and_join(std::exception_ptr failure) {
    runtime_->cancel_and_join(failure);
}

const PipelineContext& BackupPipeline::context() const {
    return *context_;
}
} // namespace pbe::detail
