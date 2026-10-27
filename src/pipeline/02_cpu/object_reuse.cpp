#include "pipeline/02_cpu/object_reuse.hpp"
#include "pipeline/shared/pipeline_context.hpp"
#include "pipeline/shared/stage_support.hpp"

namespace pbe::detail {
using Clock = PipelineClock;

std::optional<ObjectHeader> reuse_existing_object(PipelineContext& context, ChunkTask& task,
                                                  const Digest& digest, const std::string& id,
                                                  sqlite3* worker_catalog) {
    ObjectHeader existing_header;
    ObjectLocation existing_location;
    std::filesystem::path stored_path;
    bool object_exists = false;
    if (context.packed_storage) {
        auto location = detail::lookup_object_location(worker_catalog, id);
        if (location) {
            existing_location = *location;
            const auto header_begin = Clock::now();
            existing_header = detail::read_packed_object_header(context.repo, existing_location);
            context.metrics.object_io_ns.fetch_add(elapsed_ns(header_begin),
                                                   std::memory_order_relaxed);
            object_exists = true;
        }
    } else {
        stored_path = object_path(context.repo, id);
        if (std::filesystem::exists(stored_path)) {
            const auto header_begin = Clock::now();
            existing_header = read_object_header(stored_path);
            context.metrics.object_io_ns.fetch_add(elapsed_ns(header_begin),
                                                   std::memory_order_relaxed);
            object_exists = true;
        }
    }
    if (object_exists) {
        const std::size_t raw_size = task.raw.size();
        if (existing_header.digest != digest || existing_header.raw_bytes != raw_size)
            throw std::runtime_error("existing object conflicts with chunk digest");
        const bool verify_existing = !context.verified_existing_objects.contains_or_insert(id);

        // Release source bytes first so the same credit can cover stored and decompressed object buffers.
        std::vector<std::uint8_t>().swap(task.raw);
        if (verify_existing) {
            ObjectReadTimings timings;
            const auto verified_raw =
                context.packed_storage
                    ? detail::read_packed_object(context.repo, existing_location, id, &timings)
                    : read_object(stored_path, id, &timings);
            if (verified_raw.size() != raw_size)
                throw std::runtime_error("existing object content length differs from its digest");
            record_object_read(context.metrics, timings);
        }
        existing_header.newly_written = false;
        return existing_header;
    }
    return std::nullopt;
}
} // namespace pbe::detail
