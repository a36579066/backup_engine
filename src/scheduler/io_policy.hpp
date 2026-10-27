#pragma once

#include <cstddef>

namespace pbe {
struct BackupOptions;
}

namespace pbe::detail {
// Validate policy and capacity before startup; batch sizing accounts for full chunk credit.
struct IoPolicy {
    bool phased = false;
    std::size_t chunk_credit_bytes = 0;
    std::size_t batch_chunks = 0; // Streaming has no accumulated read batch.
};

IoPolicy make_io_policy(const BackupOptions& options);
} // namespace pbe::detail
