#pragma once

#include "pipeline/shared/messages.hpp"

namespace pbe::detail {
struct PipelineContext;
class MetadataBatcher;

// Control source-to-worker handoff without owning file reads, codecs, or storage formats.
// Streaming dispatches each chunk; phased mode holds a bounded batch and waits for durable ACKs.
class SourceIoScheduler {
public:
    SourceIoScheduler(PipelineContext& context, MetadataBatcher& metadata);

    // Call before raw allocation; drain a full batch first to avoid waiting on its own held credits.
    MemoryBudget::Credit acquire_credit();
    void submit(const std::string& path, std::uint64_t ordinal, std::vector<std::uint8_t> raw,
                MemoryBudget::Credit credit);
    void flush(); // Flush the final partial batch; file boundaries do not force a flush.

private:
    struct PendingRead {
        ChunkTask task;
        PendingChunk metadata;
    };

    void dispatch(PendingRead& read);
    void check_cancelled() const;

    PipelineContext& context_;
    MetadataBatcher& metadata_;
    std::vector<PendingRead> pending_;
};
} // namespace pbe::detail
