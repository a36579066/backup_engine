#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>

namespace pbe {
using Digest = std::array<std::uint8_t, 32>;

// Hash arbitrary content into a 32-byte digest used to identify identical backup chunks.
// Unkeyed BLAKE3-256. The implementation follows the public BLAKE3 specification.
Digest blake3(std::span<const std::uint8_t> input);
std::string digest_hex(const Digest& digest);
Digest digest_from_hex(const std::string& hex);
} // namespace pbe
