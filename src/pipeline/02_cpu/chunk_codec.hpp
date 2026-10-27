#pragma once

#include "pipeline/shared/messages.hpp"

namespace pbe::detail {
struct PipelineContext;

// Hashing identifies content; compression changes its representation, never its digest.
Digest hash_chunk(PipelineContext& context, const ChunkTask& task);
BlobWriteTask compress_chunk(PipelineContext& context, ChunkTask& task, const Digest& digest);
} // namespace pbe::detail
