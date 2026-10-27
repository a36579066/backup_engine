#pragma once

namespace pbe::detail {
struct PipelineContext;

// Stage 03 consumes object_queue and reports durable ACKs after successful publication.
// Loose storage uses one file per object; packs batch objects to reduce syncs and file operations.
void run_loose_object_writer(PipelineContext& context);
void run_pack_object_writer(PipelineContext& context);
} // namespace pbe::detail
