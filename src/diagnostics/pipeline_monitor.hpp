#pragma once

#include <atomic>

namespace pbe::detail {
struct PipelineContext;

// Run as a framework observer until stop or the shared cancellation flag is set.
void run_pipeline_monitor(PipelineContext& context, const std::atomic<bool>& stop);
} // namespace pbe::detail
