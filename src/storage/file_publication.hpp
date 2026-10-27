#pragma once

#include <filesystem>

namespace pbe::detail {
enum class FilePublication {
    linked,
    moved,
    already_exists
};

// Publish a fully written staging file without replacing an existing destination.
// A successful move consumes the staging path; a link leaves it for the caller to remove.
FilePublication publish_staged_file(const std::filesystem::path& staging,
                                    const std::filesystem::path& destination);
} // namespace pbe::detail
