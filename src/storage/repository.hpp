#pragma once

#include <filesystem>
#include <cstdint>

namespace pbe::detail {
std::filesystem::path canonical_existing(const std::filesystem::path& path);
bool path_within(const std::filesystem::path& child, const std::filesystem::path& parent);
void validate_repo(const std::filesystem::path& repo);
std::uint32_t repository_format_version(const std::filesystem::path& repo);

class RepoLock {
public:
    explicit RepoLock(const std::filesystem::path& repo);
    ~RepoLock();
    RepoLock(const RepoLock&) = delete;
    RepoLock& operator=(const RepoLock&) = delete;
private:
    int fd_ = -1;
};
} // namespace pbe::detail
