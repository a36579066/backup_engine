#pragma once

#include <cstddef>

namespace pbe::detail {
struct PipelineContext;

// Open a private object-index connection per CPU worker before starting threads.
void prepare_cpu_workers(PipelineContext& context);

// Stage 02 hashes, deduplicates, and compresses input chunks, forwarding new objects to Storage.
void run_chunk_worker(PipelineContext& context, std::size_t worker_index);
} // namespace pbe::detail
