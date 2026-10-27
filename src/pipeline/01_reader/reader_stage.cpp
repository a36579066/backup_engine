#include "pipeline/01_reader/reader_stage.hpp"
#include "pipeline/01_reader/cached_file_reader.hpp"
#include "pipeline/01_reader/source_file.hpp"
#include "pipeline/01_reader/source_io_scheduler.hpp"
#include "pipeline/shared/pipeline_context.hpp"
#include "pipeline/shared/stage_support.hpp"

#include <algorithm>
#include <stdexcept>

namespace pbe::detail {
namespace {
void read_file(PipelineContext& context, const SourceEntry& entry, MetadataBatcher& metadata,
               SourceIoScheduler& io) {
    // Check the opened file against scan metadata; reject changes rather than publish partial input.
    SourceFile file(entry);
    metadata.queue_entry(entry);
    std::uint64_t offset = 0, ordinal = 0;
    while (offset < entry.size) {
        const auto want =
            static_cast<std::size_t>(std::min<std::uint64_t>(kChunkSize, entry.size - offset));

        // 1. Acquire credit before allocating; downstream backpressure can pause the Reader here.
        auto credit = io.acquire_credit();
        std::vector<std::uint8_t> raw(want);
        const auto begin = PipelineClock::now();
        file.read_into(raw);
        context.metrics.source_read_ns.fetch_add(elapsed_ns(begin), std::memory_order_relaxed);

        // 2. Schedule immediate or batched handoff and register the Metadata completion result.
        io.submit(entry.relative, ordinal++, std::move(raw), std::move(credit));
        offset += want;
        context.summary.read_bytes += want;
        if (!sleep_cancellable(context.options.test_reader_delay_ms, context.errors.cancelled))
            throw std::runtime_error("backup cancelled during source read");
    }

    // Validate again after reading; count the file only after all reads and checks succeed.
    file.validate_unchanged();
    metadata.complete_file();
}
} // namespace

void read_source_files(PipelineContext& context) {
    // Skip source reads through the old snapshot only when metadata trust is explicitly enabled.
    std::string previous_snapshot;
    MetadataCache metadata_cache;
    Catalog source_catalog(context.repo / "catalog.sqlite");
    if (context.options.trust_metadata && !context.options.paranoid)
        metadata_cache =
            load_metadata_cache(source_catalog.get(), context.source_text, previous_snapshot);

    // Any scan failure aborts the backup; never accept a partial source listing as complete.
    const auto begin = PipelineClock::now();
    const auto entries =
        scan_source(context.source, context.options.skip_unsupported, context.errors.cancelled);
    context.metrics.source_scan_ns.fetch_add(elapsed_ns(begin), std::memory_order_relaxed);
    MetadataBatcher metadata(context);
    SourceIoScheduler io(context, metadata);
    for (const auto& entry : entries) {
        if (context.errors.cancelled.load(std::memory_order_relaxed))
            context.errors.rethrow();

        // Directories and symlinks need only metadata; regular files also produce chunk tasks.
        if (entry.kind == "directory") {
            metadata.queue_entry(entry);
            ++context.summary.directories;
            continue;
        }
        if (entry.kind == "symlink") {
            metadata.queue_entry(entry);
            ++context.summary.symlinks;
            continue;
        }
        ++context.summary.files;
        context.summary.logical_bytes += entry.size;
        if (can_reuse(metadata_cache, entry)) {
            io.flush();
            reuse_cached_file(context, metadata, source_catalog, previous_snapshot, entry);
        } else
            read_file(context, entry, metadata, io);
    }

    // Submit the final partial data and metadata batches before leaving the source stage.
    io.flush();
    metadata.flush_metadata();
}
} // namespace pbe::detail
