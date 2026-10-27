#include "core/backup_session.hpp"
#include "pipeline/01_reader/metadata_batcher.hpp"
#include "pipeline/01_reader/source_io_scheduler.hpp"
#include "pipeline/shared/pipeline_context.hpp"
#include "scheduler/io_policy.hpp"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <limits>
#include <thread>

namespace {
using namespace std::chrono_literals;
namespace fs = std::filesystem;

void check(bool value, const char* message) {
    if (!value)
        throw std::runtime_error(message);
}

struct Fixture {
    fs::path root = fs::temp_directory_path() /
                    ("pbe-io-scheduler-" +
                     std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));

    Fixture() {
        fs::create_directories(root / "source");
        pbe::init_repository(root / "repo", "pack-v2");
    }

    ~Fixture() {
        std::error_code error;
        fs::remove_all(root, error);
    }

    pbe::BackupOptions options() const {
        pbe::BackupOptions result;
        result.source = root / "source";
        result.repo = root / "repo";
        result.io_strategy = "phased";
        result.io_batch_mib = 2;
        result.memory_mib = 5;
        return result;
    }
};

void policy_contract() {
    pbe::BackupOptions options;
    check(pbe::detail::make_io_policy(options).batch_chunks == 0,
          "default streaming does not accumulate read batches");
    options.io_strategy = "phased";
    options.memory_mib = 3;
    options.io_batch_mib = 128;
    const auto small = pbe::detail::make_io_policy(options);
    check(small.batch_chunks == 1 && small.chunk_credit_bytes > (2u << 20),
          "batch capacity includes the worst compression allocation and clamps to credits");
    options.memory_mib = 256;
    options.io_batch_mib = 2;
    check(pbe::detail::make_io_policy(options).batch_chunks == 2,
          "explicit raw cap can reduce the credit-derived batch");
    for (int invalid = 0; invalid < 4; ++invalid) {
        auto bad = options;
        if (invalid == 0)
            bad.io_strategy = "unknown";
        if (invalid == 1)
            bad.io_strategy = "streaming";
        if (invalid == 2)
            bad.memory_mib = 2;
        if (invalid == 3)
            bad.io_batch_mib = std::numeric_limits<std::size_t>::max();
        bool rejected = false;
        try {
            (void)pbe::detail::make_io_policy(bad);
        } catch (const std::runtime_error&) {
            rejected = true;
        }
        check(rejected, "invalid I/O configuration must fail before workers start");
    }
}

// Use controlled ACKs to test handoff contracts without depending on incidental disk timing.
void batch_waits_for_every_ack(bool inject_error) {
    Fixture fixture;
    pbe::detail::BackupSession session(pbe::detail::make_backup_plan(fixture.options()));
    pbe::detail::PipelineContext context(session);
    pbe::detail::MetadataBatcher metadata(context);
    pbe::detail::SourceIoScheduler scheduler(context, metadata);
    for (std::size_t i = 0; i < 2; ++i) {
        auto credit = scheduler.acquire_credit();
        scheduler.submit("file", i, std::vector<std::uint8_t>(4, i), std::move(credit));
    }
    check(context.input_queue.size() == 0,
          "read phase must retain the batch until the next acquire or final flush");

    std::promise<void> resumed;
    auto result = resumed.get_future();
    std::thread reader([&] {
        try {
            // Drain a full batch and wait for ACKs before acquiring credit for the next chunk.
            auto next = scheduler.acquire_credit();
            resumed.set_value();
        } catch (...) {
            resumed.set_exception(std::current_exception());
        }
    });
    try {
        auto first = context.input_queue.pop();
        auto second = context.input_queue.pop();
        check(first && second, "full batch is dispatched before waiting for memory");
        check(context.metrics.io_draining.load(), "draining is observable");
        first->credit.release();
        second->credit.release();
        pbe::detail::ObjectHeader header;
        header.raw_bytes = 4;
        second->promise->set_value(header);
        check(result.wait_for(20ms) == std::future_status::timeout,
              "an out-of-order final ACK cannot release a batch with an earlier pending ACK");
        if (inject_error)
            first->promise->set_exception(
                std::make_exception_ptr(std::runtime_error("injected durable ACK failure")));
        else
            first->promise->set_value(header);
        check(result.wait_for(2s) == std::future_status::ready,
              "all ACKs or an ACK failure must release the reader promptly");
        bool failed = false;
        try {
            result.get();
        } catch (const std::runtime_error&) {
            failed = true;
        }
        check(failed == inject_error, "failed ACK is propagated to the reader");
    } catch (...) {
        context.fail(std::current_exception());
        reader.join();
        throw;
    }
    reader.join();
    check(!context.metrics.io_draining.load(), "drain phase resets on success and error");
    check(context.metrics.io_batches.load() == (inject_error ? 0u : 1u),
          "only a fully acknowledged batch is counted as complete");
    check(context.memory.current() == 0, "all credits are returned after handoff");
}
} // namespace

int main() {
    try {
        policy_contract();
        batch_waits_for_every_ack(false);
        batch_waits_for_every_ack(true);
        std::cout << "I/O scheduling contracts passed\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
