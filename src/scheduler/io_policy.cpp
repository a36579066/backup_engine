#include "scheduler/io_policy.hpp"
#include "pbe/engine.hpp"
#include "storage/object_store.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <zstd.h>

namespace pbe::detail {
IoPolicy make_io_policy(const BackupOptions& options) {
    const auto max_mib = std::numeric_limits<std::size_t>::max() >> 20;
    if (options.memory_mib < 3 || options.memory_mib > max_mib)
        throw std::runtime_error("--memory-mib must be at least 3 and fit in this process");
    if (options.io_strategy != "streaming" && options.io_strategy != "phased")
        throw std::runtime_error("--io-strategy must be streaming or phased");
    if (options.io_batch_mib > max_mib)
        throw std::runtime_error("--io-batch-mib must fit in this process");
    if (options.io_strategy == "streaming" && options.io_batch_mib != 0)
        throw std::runtime_error("--io-batch-mib requires --io-strategy phased");

    IoPolicy policy;
    policy.phased = options.io_strategy == "phased";
    policy.chunk_credit_bytes = kChunkSize + ZSTD_compressBound(kChunkSize);
    if (policy.phased) {
        const auto credit_capacity = (options.memory_mib << 20) / policy.chunk_credit_bytes;
        const std::size_t requested =
            options.io_batch_mib ? (options.io_batch_mib << 20) / kChunkSize : credit_capacity;
        policy.batch_chunks = std::min(requested, credit_capacity);
        if (policy.batch_chunks == 0)
            throw std::runtime_error("I/O batch must fit at least one chunk and its credit");
    }
    return policy;
}
} // namespace pbe::detail
