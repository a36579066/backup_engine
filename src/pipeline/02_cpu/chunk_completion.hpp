#pragma once

#include "pipeline/shared/messages.hpp"
#include "concurrency/claim_registry.hpp"

namespace pbe::detail {
struct PipelineContext;
using ChunkClaim = ClaimRegistry<ObjectHeader>::State;

// Duplicates subscribe without blocking; claim completion resolves each subscriber's promise.
void subscribe_to_duplicate(PipelineContext& context, const std::shared_ptr<ChunkClaim>& state,
                            const std::shared_ptr<std::promise<ObjectHeader>>& promise,
                            std::size_t raw_size);

// Storage completion resolves the claim and the promise observed by Metadata.
void submit_new_object(PipelineContext& context, ChunkTask& task, BlobWriteTask blob,
                       const std::string& claim_id, const std::shared_ptr<ChunkClaim>& owner_state);
} // namespace pbe::detail
