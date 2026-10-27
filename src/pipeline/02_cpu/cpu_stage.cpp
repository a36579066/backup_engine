#include "pipeline/02_cpu/cpu_stage.hpp"
#include "pipeline/02_cpu/chunk_codec.hpp"
#include "pipeline/02_cpu/chunk_completion.hpp"
#include "pipeline/02_cpu/object_reuse.hpp"
#include "pipeline/shared/pipeline_context.hpp"

namespace pbe::detail {
namespace {
void process_chunk(PipelineContext& context, ChunkTask& task, sqlite3* worker_catalog) {
    std::shared_ptr<ChunkClaim> owner_state;
    std::string claim_id;
    bool is_owner = false;
    bool permit = false;
    try {
        if (context.errors.cancelled.load(std::memory_order_relaxed))
            throw std::runtime_error("backup cancelled");

        // 1. Acquire CPU admission; adaptive control changes permits, not the existing thread pool.
        if (!context.gate.acquire(context.errors.cancelled))
            throw std::runtime_error("backup cancelled while waiting for CPU admission");
        permit = true;
        const auto digest = hash_chunk(context, task);
        claim_id = digest_hex(digest);

        // 2. Claim before lookup so concurrent duplicate chunks share one processing owner.
        auto [state, owner] = context.claims.claim(claim_id);
        is_owner = owner;
        owner_state = state;
        if (!owner) {
            // Subscribe to the owner's result, release buffers/permits, and take the next task.
            // Keep CPU workers out of disk-result waits so computation can overlap Storage.
            const auto raw_size = task.raw.size();
            std::vector<std::uint8_t>().swap(task.raw);
            task.credit.release();
            context.gate.release();
            permit = false;
            subscribe_to_duplicate(context, state, task.promise, raw_size);
            return;
        }

        // 3. Reuse an existing, verified object without compression or another write.
        if (auto existing =
                reuse_existing_object(context, task, digest, claim_id, worker_catalog)) {
            context.gate.release();
            permit = false;
            context.metrics.completed_chunks.fetch_add(1, std::memory_order_relaxed);
            context.metrics.completed_raw_bytes.fetch_add(existing->raw_bytes,
                                                          std::memory_order_relaxed);
            context.claims.complete(claim_id, owner_state, *existing);
            task.promise->set_value(*existing);
            return;
        }

        // 4. Compress new content, then release CPU admission before handing it to Storage.
        auto blob = compress_chunk(context, task, digest);
        context.gate.release();
        permit = false;
        submit_new_object(context, task, std::move(blob), claim_id, owner_state);
        is_owner = false; // The Writer callback now owns success/failure reporting.
    } catch (...) {
        // Fail claims and promises not yet transferred so Metadata cannot wait forever on an abandoned task.
        const auto failure = std::current_exception();
        if (permit)
            context.gate.release();
        if (is_owner && owner_state)
            context.claims.fail(claim_id, owner_state, failure);
        try {
            task.promise->set_exception(failure);
        } catch (...) {
        }
        context.fail(failure);
    }
}
} // namespace

void prepare_cpu_workers(PipelineContext& context) {
    if (!context.packed_storage)
        return;
    context.object_index_readers.reserve(context.workers_count);
    for (std::size_t i = 0; i < context.workers_count; ++i)
        context.object_index_readers.push_back(
            std::make_unique<Catalog>(context.repo / "catalog.sqlite"));
}

void run_chunk_worker(PipelineContext& context, std::size_t worker_index) {
    sqlite3* catalog =
        context.packed_storage ? context.object_index_readers.at(worker_index)->get() : nullptr;

    // Consume the queue filled by Reader input_queue.push; CPU workers share this input.
    while (auto task = context.input_queue.pop(&context.errors.cancelled))
        process_chunk(context, *task, catalog);
}
} // namespace pbe::detail
