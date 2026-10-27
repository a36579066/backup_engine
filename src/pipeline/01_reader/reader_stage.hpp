#pragma once

namespace pbe::detail {
struct PipelineContext;

// Stage 01 scans and chunks on the caller thread, feeding CPU and Metadata branches.
void read_source_files(PipelineContext& context);
} // namespace pbe::detail
