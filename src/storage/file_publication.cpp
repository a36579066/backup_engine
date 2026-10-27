#include "storage/file_publication.hpp"

#include <cerrno>
#include <system_error>
#include <unistd.h>

#ifdef __APPLE__
#include <stdio.h>
#endif

namespace pbe::detail {
FilePublication publish_staged_file(const std::filesystem::path& staging,
                                    const std::filesystem::path& destination) {
    if (::link(staging.c_str(), destination.c_str()) == 0)
        return FilePublication::linked;

    int error = errno;
    if (error == EEXIST)
        return FilePublication::already_exists;

#ifdef __APPLE__
    // FAT32/exFAT may reject hard links. RENAME_EXCL preserves no-replace publication
    // without a check-then-rename race or an extra copy of the stored payload.
    if (error == ENOTSUP || error == EOPNOTSUPP || error == ENOSYS) {
        if (::renamex_np(staging.c_str(), destination.c_str(), RENAME_EXCL) == 0)
            return FilePublication::moved;
        error = errno;
        if (error == EEXIST)
            return FilePublication::already_exists;
    }
#endif

    // Permission failures and unsupported exclusive renames remain fatal. Never fall back
    // to a plain rename, which could silently replace a complete repository object.
    throw std::system_error(error, std::generic_category(),
                            "publish repository file " + destination.string());
}
} // namespace pbe::detail
