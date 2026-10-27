#pragma once

#include "pipeline/shared/messages.hpp"
#include "pipeline/01_reader/source_scanner.hpp"

namespace pbe::detail {
struct PipelineContext;

// Reader-owned batch accumulator that reduces queue handoffs and SQLite transactions.
// queue_chunk stores the future without waiting for Storage to finish.
class MetadataBatcher {
public:
    explicit MetadataBatcher(PipelineContext& owner)
        : context(owner) {
    }

    void queue_entry(const SourceEntry& entry);
    void queue_chunk(const std::string& path, std::uint64_t ordinal, std::uint64_t bytes,
                     std::shared_future<ObjectHeader> result);

    void complete_file() {
        ++pending_metadata.completed_files;
    }

    void flush_metadata();

private:
    PipelineContext& context;
    MetadataBatch pending_metadata;
};
} // namespace pbe::detail
