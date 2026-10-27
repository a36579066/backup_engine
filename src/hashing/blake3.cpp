#include "pbe/blake3.hpp"

#include <algorithm>
#include <bit>
#include <stdexcept>

namespace pbe {
namespace {
constexpr std::array<std::uint32_t, 8> kIv = {0x6A09E667u, 0xBB67AE85u, 0x3C6EF372u, 0xA54FF53Au,
                                              0x510E527Fu, 0x9B05688Cu, 0x1F83D9ABu, 0x5BE0CD19u};
constexpr std::array<std::uint8_t, 16> kPermutation = {2, 6,  3,  10, 7, 0,  4,  13,
                                                       1, 11, 12, 5,  9, 14, 15, 8};
constexpr std::uint32_t kChunkStart = 1u;
constexpr std::uint32_t kChunkEnd = 2u;
constexpr std::uint32_t kParent = 4u;
constexpr std::uint32_t kRoot = 8u;
constexpr std::size_t kBlockLen = 64;

// BLAKE3's internal 1 KiB chunks are distinct from the backup pipeline's 1 MiB chunks.
constexpr std::size_t kChunkLen = 1024;

std::uint32_t load32(const std::uint8_t* p) {
    return std::uint32_t(p[0]) | (std::uint32_t(p[1]) << 8) | (std::uint32_t(p[2]) << 16) |
           (std::uint32_t(p[3]) << 24);
}

void store32(std::uint8_t* p, std::uint32_t x) {
    p[0] = static_cast<std::uint8_t>(x);
    p[1] = static_cast<std::uint8_t>(x >> 8);
    p[2] = static_cast<std::uint8_t>(x >> 16);
    p[3] = static_cast<std::uint8_t>(x >> 24);
}

void g(std::array<std::uint32_t, 16>& s, int a, int b, int c, int d, std::uint32_t mx,
       std::uint32_t my) {
    s[a] = s[a] + s[b] + mx;
    s[d] = std::rotr(s[d] ^ s[a], 16);
    s[c] += s[d];
    s[b] = std::rotr(s[b] ^ s[c], 12);
    s[a] = s[a] + s[b] + my;
    s[d] = std::rotr(s[d] ^ s[a], 8);
    s[c] += s[d];
    s[b] = std::rotr(s[b] ^ s[c], 7);
}

std::array<std::uint32_t, 16> compress(const std::array<std::uint32_t, 8>& cv,
                                       const std::array<std::uint32_t, 16>& block,
                                       std::uint64_t counter, std::uint32_t block_len,
                                       std::uint32_t flags) {
    std::array<std::uint32_t, 16> s{};
    std::copy(cv.begin(), cv.end(), s.begin());
    std::copy_n(kIv.begin(), 4, s.begin() + 8);
    s[12] = static_cast<std::uint32_t>(counter);
    s[13] = static_cast<std::uint32_t>(counter >> 32);
    s[14] = block_len;
    s[15] = flags;
    auto m = block;
    for (int round = 0; round < 7; ++round) {
        g(s, 0, 4, 8, 12, m[0], m[1]);
        g(s, 1, 5, 9, 13, m[2], m[3]);
        g(s, 2, 6, 10, 14, m[4], m[5]);
        g(s, 3, 7, 11, 15, m[6], m[7]);
        g(s, 0, 5, 10, 15, m[8], m[9]);
        g(s, 1, 6, 11, 12, m[10], m[11]);
        g(s, 2, 7, 8, 13, m[12], m[13]);
        g(s, 3, 4, 9, 14, m[14], m[15]);
        std::array<std::uint32_t, 16> next{};
        for (std::size_t i = 0; i < 16; ++i)
            next[i] = m[kPermutation[i]];
        m = next;
    }
    for (std::size_t i = 0; i < 8; ++i) {
        s[i] ^= s[i + 8];
        s[i + 8] ^= cv[i];
    }
    return s;
}

struct Output {
    std::array<std::uint32_t, 8> cv;
    std::array<std::uint32_t, 16> block;
    std::uint64_t counter;
    std::uint32_t block_len;
    std::uint32_t flags;

    std::array<std::uint32_t, 8> chaining_value() const {
        const auto words = compress(cv, block, counter, block_len, flags);
        std::array<std::uint32_t, 8> out{};
        std::copy_n(words.begin(), 8, out.begin());
        return out;
    }

    Digest root_digest() const {
        const auto words = compress(cv, block, 0, block_len, flags | kRoot);
        Digest out{};
        for (std::size_t i = 0; i < 8; ++i)
            store32(out.data() + i * 4, words[i]);
        return out;
    }
};

std::array<std::uint32_t, 16> block_words(std::span<const std::uint8_t> bytes) {
    std::array<std::uint8_t, kBlockLen> padded{};
    std::copy(bytes.begin(), bytes.end(), padded.begin());
    std::array<std::uint32_t, 16> words{};
    for (std::size_t i = 0; i < words.size(); ++i)
        words[i] = load32(padded.data() + i * 4);
    return words;
}

Output chunk_output(std::span<const std::uint8_t> chunk, std::uint64_t chunk_counter) {
    auto cv = kIv;
    const std::size_t blocks = std::max<std::size_t>(1, (chunk.size() + kBlockLen - 1) / kBlockLen);
    for (std::size_t i = 0; i + 1 < blocks; ++i) {
        auto words = block_words(chunk.subspan(i * kBlockLen, kBlockLen));
        const auto out = compress(cv, words, chunk_counter, kBlockLen, i == 0 ? kChunkStart : 0u);
        std::copy_n(out.begin(), 8, cv.begin());
    }
    const std::size_t offset = (blocks - 1) * kBlockLen;
    const std::size_t final_len = chunk.size() - std::min(chunk.size(), offset);
    auto words = block_words(chunk.subspan(offset, final_len));
    const std::uint32_t flags = kChunkEnd | (blocks == 1 ? kChunkStart : 0u);
    return Output{cv, words, chunk_counter, static_cast<std::uint32_t>(final_len), flags};
}

Output parent_output(const std::array<std::uint32_t, 8>& left,
                     const std::array<std::uint32_t, 8>& right) {
    std::array<std::uint32_t, 16> block{};
    std::copy(left.begin(), left.end(), block.begin());
    std::copy(right.begin(), right.end(), block.begin() + 8);
    return Output{kIv, block, 0, static_cast<std::uint32_t>(kBlockLen), kParent};
}
} // namespace

Digest blake3(std::span<const std::uint8_t> input) {
    const std::size_t chunks = std::max<std::size_t>(1, (input.size() + kChunkLen - 1) / kChunkLen);
    std::array<std::array<std::uint32_t, 8>, 64> stack{};
    std::size_t stack_len = 0;
    for (std::size_t i = 0; i + 1 < chunks; ++i) {
        auto cv = chunk_output(input.subspan(i * kChunkLen, kChunkLen), i).chaining_value();
        std::size_t total = i + 1;
        while ((total & 1u) == 0u) {
            cv = parent_output(stack[--stack_len], cv).chaining_value();
            total >>= 1;
        }
        stack[stack_len++] = cv;
    }
    const std::size_t last_offset = (chunks - 1) * kChunkLen;
    Output output = chunk_output(input.subspan(last_offset), chunks - 1);
    while (stack_len > 0)
        output = parent_output(stack[--stack_len], output.chaining_value());
    return output.root_digest();
}

std::string digest_hex(const Digest& digest) {
    static constexpr char hex[] = "0123456789abcdef";
    std::string result;
    result.resize(64);
    for (std::size_t i = 0; i < digest.size(); ++i) {
        result[i * 2] = hex[digest[i] >> 4];
        result[i * 2 + 1] = hex[digest[i] & 0x0f];
    }
    return result;
}

Digest digest_from_hex(const std::string& hex) {
    if (hex.size() != 64)
        throw std::invalid_argument("digest must contain 64 hex characters");
    auto digit = [](char c) -> int {
        if (c >= '0' && c <= '9')
            return c - '0';
        if (c >= 'a' && c <= 'f')
            return c - 'a' + 10;
        if (c >= 'A' && c <= 'F')
            return c - 'A' + 10;
        return -1;
    };
    Digest out{};
    for (std::size_t i = 0; i < out.size(); ++i) {
        const int hi = digit(hex[2 * i]), lo = digit(hex[2 * i + 1]);
        if (hi < 0 || lo < 0)
            throw std::invalid_argument("invalid digest hex");
        out[i] = static_cast<std::uint8_t>((hi << 4) | lo);
    }
    return out;
}
} // namespace pbe
