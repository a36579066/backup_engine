#pragma once

namespace pbe::detail {
struct PipelineContext;

// Stage 04 is the sole catalog writer during the pipeline and commits Reader batches in order.
void run_metadata_writer(PipelineContext& context);
} // namespace pbe::detail
