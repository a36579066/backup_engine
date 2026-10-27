#pragma once

#include <span>

#include "pipeline/01_reader/source_scanner.hpp"

namespace pbe::detail {
// RAII owns the open source descriptor and closes it on normal, cancelled, and exceptional exits.
class SourceFile {
public:
    explicit SourceFile(const SourceEntry& entry);
    ~SourceFile();
    SourceFile(const SourceFile&) = delete;
    SourceFile& operator=(const SourceFile&) = delete;
    void read_into(std::span<std::uint8_t> bytes);
    void validate_unchanged() const;

private:
    int fd_;
    struct stat before_{};
    std::string relative_;
};
} // namespace pbe::detail
