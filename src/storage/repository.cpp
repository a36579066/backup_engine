#include "storage/repository.hpp"

#include "storage/object_store.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>

namespace pbe::detail {
namespace {
struct RepoConfig {
    std::uint32_t format_version = 1;
    std::uint64_t chunk_size = kChunkSize;
    std::string hash_algorithm = "blake3-256";
    std::string storage_format = "files-v1";
};

std::string error_text(std::string_view operation) {
    return std::string(operation) + ": " + std::strerror(errno);
}

RepoConfig read_config(const std::filesystem::path& repo) {
    // Validate version, format, chunk size, and hash from repo.toml before reading objects.
    const auto path = repo / "repo.toml";
    std::ifstream in(path);
    if (!in)
        throw std::runtime_error("not a PBE repository: missing " + path.string());
    RepoConfig config;
    bool format_seen = false, chunk_seen = false, hash_seen = false;
    std::string line;
    while (std::getline(in, line)) {
        const auto eq = line.find('=');
        if (eq == std::string::npos)
            continue;
        std::string key = line.substr(0, eq);
        std::string value = line.substr(eq + 1);
        key.erase(std::remove_if(key.begin(), key.end(),
                                 [](unsigned char c) {
                                     return c == ' ' || c == '\t' || c == '\r';
                                 }),
                  key.end());
        value.erase(std::remove_if(value.begin(), value.end(),
                                   [](unsigned char c) {
                                       return c == ' ' || c == '\t' || c == '\r';
                                   }),
                    value.end());
        if (key == "format_version") {
            config.format_version = static_cast<std::uint32_t>(std::stoul(value));
            format_seen = true;
        } else if (key == "chunk_size") {
            config.chunk_size = std::stoull(value);
            chunk_seen = true;
        } else if (key == "hash_algorithm") {
            if (value.size() >= 2 && value.front() == '"' && value.back() == '"')
                value = value.substr(1, value.size() - 2);
            config.hash_algorithm = value;
            hash_seen = true;
        } else if (key == "storage_format") {
            if (value.size() >= 2 && value.front() == '"' && value.back() == '"')
                value = value.substr(1, value.size() - 2);
            config.storage_format = value;
        }
    }
    if (!format_seen || !chunk_seen || !hash_seen)
        throw std::runtime_error("repository config is incomplete");
    if (config.format_version != 1 && config.format_version != 2)
        throw std::runtime_error("unsupported repository format version " +
                                 std::to_string(config.format_version));
    if ((config.format_version == 1 && config.storage_format != "files-v1") ||
        (config.format_version == 2 && config.storage_format != "pack-v2"))
        throw std::runtime_error("repository format version and storage format do not match");
    if (config.chunk_size != kChunkSize || config.hash_algorithm != "blake3-256")
        throw std::runtime_error("unsupported repository chunk/hash configuration");
    return config;
}
} // namespace

std::filesystem::path canonical_existing(const std::filesystem::path& path) {
    std::error_code ec;
    auto result = std::filesystem::canonical(path, ec);
    if (ec)
        throw std::runtime_error("cannot resolve path " + path.string() + ": " + ec.message());
    return result;
}

bool path_within(const std::filesystem::path& child, const std::filesystem::path& parent) {
    auto c = child.begin(), p = parent.begin();
    for (; p != parent.end(); ++p, ++c) {
        if (c == child.end() || *c != *p)
            return false;
    }
    return true;
}

void validate_repo(const std::filesystem::path& repo) {
    std::error_code ec;
    if (!std::filesystem::is_directory(repo, ec) || ec)
        throw std::runtime_error("repository directory does not exist: " + repo.string());
    (void)read_config(repo);
    if (!std::filesystem::exists(repo / "catalog.sqlite"))
        throw std::runtime_error("repository catalog is missing");
}

std::uint32_t repository_format_version(const std::filesystem::path& repo) {
    return read_config(repo).format_version;
}

RepoLock::RepoLock(const std::filesystem::path& repo) {
    const auto path = repo / ".repo.lock";
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd_ < 0)
        throw std::runtime_error(error_text("open repository lock"));

    // Take an exclusive, nonblocking writer lock; reject concurrent catalog/staging mutation.
    if (::flock(fd_, LOCK_EX | LOCK_NB) != 0) {
        const int saved = errno;
        ::close(fd_);
        fd_ = -1;
        errno = saved;
        if (saved == EWOULDBLOCK || saved == EAGAIN)
            throw std::runtime_error("repository writer lock is already held");
        throw std::runtime_error(error_text("lock repository"));
    }
}

RepoLock::~RepoLock() {
    if (fd_ >= 0) {
        ::flock(fd_, LOCK_UN);
        ::close(fd_);
    }
}
} // namespace pbe::detail
