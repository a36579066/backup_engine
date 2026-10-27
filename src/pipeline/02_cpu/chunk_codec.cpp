#include "pipeline/02_cpu/chunk_codec.hpp"
#include "pipeline/shared/pipeline_context.hpp"
#include "pipeline/shared/stage_support.hpp"

#include <zstd.h>
#include <stdexcept>

namespace pbe::detail {
using Clock = PipelineClock;

Digest hash_chunk(PipelineContext& context, const ChunkTask& task) {
    const auto hash_begin = Clock::now();
    const Digest digest = blake3(task.raw);
    const auto hash_ns = elapsed_ns(hash_begin);
    context.metrics.hash_ns.fetch_add(hash_ns, std::memory_order_relaxed);
    context.metrics.cpu_busy_ns.fetch_add(hash_ns, std::memory_order_relaxed);

    // Injected CPU work is test-only, normally zero, and timed separately from real hashing/compression.
    const std::uint64_t digest_seed = get_le(digest.data(), 8);
    const auto spin_begin = Clock::now();
    const bool spin_ok = spin_cpu_cancellable(context.options.test_cpu_spin_ms,
                                              context.errors.cancelled, digest_seed);
    const auto spin_ns = elapsed_ns(spin_begin);
    context.metrics.controlled_cpu_ns.fetch_add(spin_ns, std::memory_order_relaxed);
    context.metrics.cpu_busy_ns.fetch_add(spin_ns, std::memory_order_relaxed);
    if (!spin_ok)
        throw std::runtime_error("backup cancelled during controlled CPU work");
    return digest;
}

BlobWriteTask compress_chunk(PipelineContext& context, ChunkTask& task, const Digest& digest) {
    std::vector<std::uint8_t> compressed(ZSTD_compressBound(task.raw.size()));
    const auto compression_begin = Clock::now();
    const std::size_t compressed_size =
        ZSTD_compress(compressed.data(), compressed.size(), task.raw.data(), task.raw.size(),
                      context.options.zstd_level);
    const auto compression_ns = elapsed_ns(compression_begin);
    context.metrics.compression_ns.fetch_add(compression_ns, std::memory_order_relaxed);
    context.metrics.cpu_busy_ns.fetch_add(compression_ns, std::memory_order_relaxed);
    if (ZSTD_isError(compressed_size))
        throw std::runtime_error(std::string("zstd compression failed: ") +
                                 ZSTD_getErrorName(compressed_size));
    BlobWriteTask blob;
    blob.digest = digest;
    blob.raw_bytes = task.raw.size();

    // Store raw bytes when compression does not shrink the payload.
    if (compressed_size < task.raw.size()) {
        compressed.resize(compressed_size);
        blob.codec = 1;
        blob.stored = std::move(compressed);
    } else {
        blob.codec = 0;
        blob.stored = std::move(task.raw);
    }
    return blob;
}
} // namespace pbe::detail
