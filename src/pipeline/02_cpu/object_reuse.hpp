#pragma once

#include <optional>
#include <sqlite3.h>

#include "pipeline/shared/messages.hpp"

namespace pbe::detail {
struct PipelineContext;

// Validate existing headers/content as needed; return nullopt when the object is absent.
std::optional<ObjectHeader> reuse_existing_object(PipelineContext& context, ChunkTask& task,
                                                  const Digest& digest, const std::string& id,
                                                  sqlite3* worker_catalog);
} // namespace pbe::detail
