#include "storage/file_publication.hpp"
#include "storage/catalog.hpp"

#include <array>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <latch>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <vector>
#include <unistd.h>

namespace {
namespace fs = std::filesystem;
using pbe::detail::FilePublication;
using pbe::detail::publish_staged_file;

void check(bool condition, const char* message) {
    if (!condition)
        throw std::runtime_error(message);
}

struct Fixture {
    fs::path root;

    explicit Fixture(const fs::path& parent) {
        std::string pattern = (parent / "pbe-publication-XXXXXX").string();
        std::vector<char> buffer(pattern.begin(), pattern.end());
        buffer.push_back('\0');
        const char* directory = ::mkdtemp(buffer.data());
        if (!directory)
            throw std::runtime_error("cannot create publication test directory");
        root = directory;
    }

    ~Fixture() {
        std::error_code error;
        fs::remove_all(root, error);
    }
};

void write(const fs::path& path, const std::string& value) {
    std::ofstream output(path, std::ios::binary);
    output.write(value.data(), static_cast<std::streamsize>(value.size()));
    output.close();
    check(static_cast<bool>(output), "cannot write publication fixture");
}

std::string read(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    check(static_cast<bool>(input), "cannot read published file");
    return {std::istreambuf_iterator<char>(input), {}};
}

void publication_contract(const fs::path& parent) {
    Fixture fixture(parent);
    const auto staging = fixture.root / "staging";
    const auto destination = fixture.root / "published";
    const std::string payload(128 * 1024, 'A');
    write(staging, payload);

    const auto result = publish_staged_file(staging, destination);
    check(result != FilePublication::already_exists, "a new file must publish successfully");
    check(read(destination) == payload, "publication must expose the complete staged payload");
    check(fs::exists(staging) == (result == FilePublication::linked),
          "publication reports whether the caller still owns a staging name");
    std::cout << "Publication method: "
              << (result == FilePublication::linked ? "hard link" : "exclusive rename") << '\n';

    const auto duplicate = fixture.root / "duplicate";
    write(duplicate, "must not replace the first payload");
    check(publish_staged_file(duplicate, destination) == FilePublication::already_exists,
          "a pre-existing destination must report a collision");
    check(read(destination) == payload, "a collision must preserve the existing destination");
    check(read(duplicate) == "must not replace the first payload",
          "a collision must leave the staging file for caller cleanup");

    bool failed = false;
    try {
        (void)publish_staged_file(fixture.root / "missing", fixture.root / "never-published");
    } catch (const std::system_error&) {
        failed = true;
    }
    check(failed && !fs::exists(fixture.root / "never-published"),
          "a missing staging file must fail without creating a destination");
}

void concurrent_publication(const fs::path& parent) {
    Fixture fixture(parent);
    constexpr std::size_t writers = 8;
    std::array<std::string, writers> payloads;
    std::array<FilePublication, writers> results{};
    std::array<std::exception_ptr, writers> errors{};
    std::latch ready(writers);
    std::latch start(1);
    std::vector<std::thread> threads;
    const auto destination = fixture.root / "winner";
    for (std::size_t i = 0; i < writers; ++i) {
        payloads[i] = std::string(64 * 1024, static_cast<char>('a' + i));
        write(fixture.root / std::to_string(i), payloads[i]);
    }
    for (std::size_t i = 0; i < writers; ++i) {
        threads.emplace_back([&, i] {
            ready.count_down();
            start.wait();
            try {
                results[i] = publish_staged_file(fixture.root / std::to_string(i), destination);
            } catch (...) {
                errors[i] = std::current_exception();
            }
        });
    }
    ready.wait();
    start.count_down();
    for (auto& thread : threads)
        thread.join();

    std::size_t winners = 0;
    for (std::size_t i = 0; i < writers; ++i) {
        if (errors[i])
            std::rethrow_exception(errors[i]);
        if (results[i] != FilePublication::already_exists) {
            ++winners;
            check(read(destination) == payloads[i], "the winning payload must remain unchanged");
        } else {
            check(read(fixture.root / std::to_string(i)) == payloads[i],
                  "losing writers must retain their complete staging files");
        }
    }
    check(winners == 1, "concurrent publication must have exactly one winner");
}

void symlink_destination(const fs::path& parent) {
    Fixture fixture(parent);
    const auto staging = fixture.root / "staging";
    const auto destination = fixture.root / "published";
    write(staging, "new payload");
    std::error_code error;
    fs::create_symlink("missing-target", destination, error);
    if (error == std::errc::operation_not_supported) {
        std::cout << "Symlink case skipped: filesystem does not support symlinks\n";
        return;
    }
    if (error)
        throw fs::filesystem_error("create symlink fixture", destination, error);
    check(publish_staged_file(staging, destination) == FilePublication::already_exists,
          "a dangling symlink must count as an existing destination");
    check(fs::is_symlink(destination) && fs::read_symlink(destination) == "missing-target",
          "publication must not follow or replace a destination symlink");
    check(!fs::exists(fixture.root / "missing-target"),
          "publication must not create a symlink target");
}

void catalog_reader_contract(const fs::path& parent) {
    Fixture fixture(parent);
    const auto path = fixture.root / "catalog.sqlite";
    {
        pbe::detail::Catalog writer(path, true);
        pbe::detail::exec(writer.get(),
                          "CREATE TABLE example(value INTEGER); INSERT INTO example VALUES(1);");
    }
    pbe::detail::Catalog reader(path);
    pbe::detail::Statement query_only(reader.get(), "PRAGMA query_only");
    check(query_only.row() && query_only.i64(0) == 1,
          "catalog query connections must enforce query-only SQL");
    bool rejected = false;
    try {
        pbe::detail::exec(reader.get(), "INSERT INTO example VALUES(2)");
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    check(rejected, "query-only catalog connections must reject metadata writes");
    pbe::detail::Statement count(reader.get(), "SELECT COUNT(*) FROM example");
    check(count.row() && count.i64(0) == 1,
          "a rejected reader write must leave metadata unchanged");
}
} // namespace

int main(int argc, char** argv) {
    try {
        if (argc > 2)
            throw std::runtime_error(
                "usage: pbe_file_publication_tests [existing-parent-directory]");
        const fs::path parent = argc == 2 ? fs::path(argv[1]) : fs::temp_directory_path();
        check(fs::is_directory(parent), "publication test parent must already exist");
        publication_contract(parent);
        concurrent_publication(parent);
        symlink_destination(parent);
        catalog_reader_contract(parent);
        std::cout << "File publication tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
