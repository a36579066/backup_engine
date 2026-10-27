#include "pipeline/00_framework/pipeline_runtime.hpp"
#include "concurrency/worker_error.hpp"
#include "pbe/bounded_queue.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {
using pbe::BoundedQueue;
using pbe::detail::PipelineRuntime;
using pbe::detail::WorkerError;

void check(bool value, const char* message) {
    if (!value)
        throw std::runtime_error(message);
}

template <class Exception, class Work>
void rejects(Work&& work, const char* message) {
    try {
        work();
    } catch (const Exception&) {
        return;
    }
    throw std::runtime_error(message);
}

void stages_overlap_and_drain_in_order() {
    WorkerError errors;
    BoundedQueue<int> input(1), output(1);
    std::mutex results_mutex, observer_mutex, shutdown_mutex;
    std::condition_variable observer_cv;
    std::vector<int> results;
    std::vector<std::string> shutdown;
    const auto record_shutdown = [&](const char* name) {
        std::lock_guard lock(shutdown_mutex);
        shutdown.push_back(name);
    };
    bool stop = false;
    std::atomic<bool> source_running{false}, overlapped{false}, observer_exited{false};
    std::promise<void> first_item;
    auto first_seen = first_item.get_future();
    const auto caller_thread = std::this_thread::get_id();
    int source_stops = 0;
    PipelineRuntime runtime(errors);
    runtime.set_source({
        .name = "source",
        .run =
            [&] {
                check(std::this_thread::get_id() == caller_thread,
                      "registered source executes on the caller thread");
                source_running.store(true);
                check(input.push(0, &errors.cancelled), "submit the first item");
                check(first_seen.wait_for(std::chrono::seconds(5)) == std::future_status::ready,
                      "worker must execute while the source is still running");
                for (int i = 1; i < 100; ++i)
                    if (!input.push(i, &errors.cancelled))
                        break;
                source_running.store(false);
            },
        .request_stop =
            [&] {
                ++source_stops;
            },
    });
    runtime.add_stage({"transform", 3,
                       [&](std::size_t) {
                           while (auto value = input.pop(&errors.cancelled)) {
                               if (*value == 0) {
                                   overlapped.store(source_running.load());
                                   first_item.set_value();
                               }
                               if (!output.push(*value * 2, &errors.cancelled))
                                   break;
                           }
                       },
                       [&] {
                           record_shutdown("transform");
                           input.close();
                       }});
    runtime.add_stage({"sink", 2,
                       [&](std::size_t) {
                           while (auto value = output.pop(&errors.cancelled)) {
                               std::lock_guard lock(results_mutex);
                               results.push_back(*value);
                           }
                       },
                       [&] {
                           record_shutdown("sink");
                           output.close();
                       }});
    runtime.add_observer({"observer",
                          [&] {
                              std::unique_lock lock(observer_mutex);
                              observer_cv.wait(lock, [&] {
                                  return stop;
                              });
                              observer_exited.store(true);
                          },
                          [&] {
                              {
                                  std::lock_guard lock(observer_mutex);
                                  stop = true;
                              }
                              record_shutdown("observer");
                              observer_cv.notify_all();
                          }});
    runtime.run();
    check(overlapped.load(), "source and worker stage actually overlap");
    check(observer_exited.load(), "finish joins observers");
    check(source_stops == 0, "normal completion does not cancel the completed source");
    check(shutdown == std::vector<std::string>{"transform", "sink", "observer"},
          "upstream drains before downstream closes, then observers stop");
    std::sort(results.begin(), results.end());
    check(results.size() == 100, "bounded stages drain every submitted item");
    for (int i = 0; i < 100; ++i)
        check(results[i] == i * 2, "stage result is correct");
}

void worker_failure_unblocks_source_and_preserves_first_error() {
    WorkerError errors;
    BoundedQueue<int> input(1), output(1);
    std::atomic<bool> sink_exited{false};
    std::atomic<int> cancel_calls{0};
    std::string failure;
    {
        PipelineRuntime runtime(errors, [&] {
            ++cancel_calls;
            throw std::runtime_error("secondary cleanup failure");
        });
        runtime.set_source({.name = "source", .run = [&] {
                                for (int i = 0; i < 10000; ++i)
                                    if (!input.push(i, &errors.cancelled))
                                        break;
                            }});
        runtime.add_stage({"broken", 1,
                           [&](std::size_t) {
                               if (input.pop(&errors.cancelled))
                                   throw std::runtime_error("original worker failure");
                           },
                           [&] {
                               input.close();
                           }});
        runtime.add_stage({"waiting sink", 1,
                           [&](std::size_t) {
                               while (output.pop(&errors.cancelled)) {
                               }
                               sink_exited.store(true);
                           },
                           [&] {
                               output.close();
                           }});
        try {
            runtime.run();
        } catch (const std::runtime_error& error) {
            failure = error.what();
        }
    }
    check(failure == "original worker failure", "cleanup must preserve the first worker error");
    check(sink_exited.load(), "failure wakes an empty downstream consumer");
    check(cancel_calls.load() == 1, "cancellation resource callback runs once");
    check(errors.error_count.load() == 1, "secondary errors do not replace the primary failure");
}

void source_failure_cancels_waiting_workers() {
    WorkerError errors;
    BoundedQueue<int> input(1);
    std::atomic<bool> exited{false};
    std::string failure;
    {
        PipelineRuntime runtime(errors);
        runtime.set_source({.name = "source", .run = [] {
                                throw std::runtime_error("source failure");
                            }});
        runtime.add_stage({"consumer", 1,
                           [&](std::size_t) {
                               while (input.pop(&errors.cancelled)) {
                               }
                               exited.store(true);
                           },
                           [&] {
                               input.close();
                           }});
        runtime.start();
        try {
            runtime.run_source();
        } catch (const std::runtime_error& error) {
            failure = error.what();
        }
    }
    check(failure == "source failure", "source exceptions reach the caller");
    check(exited.load(), "scope exit joins consumers after a source exception");
}

void preparation_failure_cleans_up_without_starting_workers() {
    WorkerError errors;
    std::atomic<int> executions{0}, cleanup_calls{0};
    PipelineRuntime runtime(errors, [&] {
        ++cleanup_calls;
    });
    runtime.set_source({.name = "source", .run = [&] {
                            ++executions;
                        }});
    runtime.add_stage({"unused", 1,
                       [&](std::size_t) {
                           ++executions;
                       },
                       [] {
                       }});
    std::string failure;
    try {
        runtime.start([] {
            throw std::runtime_error("preparation failure");
        });
    } catch (const std::runtime_error& error) {
        failure = error.what();
    }
    check(failure == "preparation failure", "preparation errors are preserved");
    check(executions.load() == 0 && cleanup_calls.load() == 1,
          "failed preparation cleans resources before any worker starts");
    runtime.cancel_and_join();
    check(cleanup_calls.load() == 1, "repeated cancellation is harmless");
}

void observer_failure_cancels_the_pipeline() {
    WorkerError errors;
    BoundedQueue<int> input(1);
    std::atomic<bool> exited{false};
    std::string failure;
    {
        PipelineRuntime runtime(errors);
        runtime.set_source({.name = "source", .run = [] {
                            }});
        runtime.add_stage({"consumer", 1,
                           [&](std::size_t) {
                               while (input.pop(&errors.cancelled)) {
                               }
                               exited.store(true);
                           },
                           [&] {
                               input.close();
                           }});
        runtime.add_observer({"broken observer",
                              [] {
                                  throw std::runtime_error("observer failure");
                              },
                              [] {
                              }});
        try {
            runtime.run();
        } catch (const std::runtime_error& error) {
            failure = error.what();
        }
    }
    check(failure == "observer failure", "observer errors propagate to the caller");
    check(exited.load(), "observer failure does not leave worker threads alive");
}

void scope_exit_cancels_without_finish() {
    WorkerError errors;
    BoundedQueue<int> input(1);
    std::atomic<bool> exited{false};
    const auto begin = std::chrono::steady_clock::now();
    {
        PipelineRuntime runtime(errors);
        runtime.set_source({.name = "source", .run = [] {
                            }});
        runtime.add_stage({"waiting consumer", 1,
                           [&](std::size_t) {
                               while (input.pop(&errors.cancelled)) {
                               }
                               exited.store(true);
                           },
                           [&] {
                               input.close();
                           }});
        runtime.start();
    }
    check(exited.load(), "destructor joins unfinished stages");
    check(std::chrono::steady_clock::now() - begin < std::chrono::seconds(5),
          "destruction promptly wakes blocked consumers");
}

void configuration_and_lifecycle_are_checked() {
    WorkerError errors;
    BoundedQueue<int> input(1);
    PipelineRuntime runtime(errors);
    rejects<std::logic_error>(
        [&] {
            runtime.start();
        },
        "empty runtime must not start");
    rejects<std::invalid_argument>(
        [&] {
            runtime.add_stage({"zero", 0,
                               [](std::size_t) {
                               },
                               [] {
                               }});
        },
        "zero-worker stages must be rejected");
    runtime.add_stage({"consumer", 1,
                       [&](std::size_t) {
                           while (input.pop(&errors.cancelled)) {
                           }
                       },
                       [&] {
                           input.close();
                       }});
    rejects<std::logic_error>(
        [&] {
            runtime.start();
        },
        "a worker stage still needs a source");
    rejects<std::invalid_argument>(
        [&] {
            runtime.set_source({.name = "", .run = [] {
                                }});
        },
        "source name must not be empty");
    rejects<std::invalid_argument>(
        [&] {
            runtime.set_source({.name = "source", .run = {}});
        },
        "source needs a run callback");
    rejects<std::invalid_argument>(
        [&] {
            runtime.set_source({.name = "consumer", .run = [] {
                                }});
        },
        "source name must not conflict with a worker");
    runtime.set_source({.name = "source", .run = [] {
                        }});
    rejects<std::logic_error>(
        [&] {
            runtime.set_source({.name = "second", .run = [] {
                                }});
        },
        "only one source can be registered");
    rejects<std::invalid_argument>(
        [&] {
            runtime.add_stage({"source", 1,
                               [](std::size_t) {
                               },
                               [] {
                               }});
        },
        "worker name must not conflict with the source");
    rejects<std::invalid_argument>(
        [&] {
            runtime.add_observer({"consumer",
                                  [] {
                                  },
                                  [] {
                                  }});
        },
        "stage and observer names must be unique");
    rejects<std::logic_error>(
        [&] {
            runtime.finish();
        },
        "finish before start must be rejected");
    runtime.start();
    rejects<std::logic_error>(
        [&] {
            runtime.start();
        },
        "duplicate start must be rejected");
    rejects<std::logic_error>(
        [&] {
            runtime.add_stage({"late", 1,
                               [](std::size_t) {
                               },
                               [] {
                               }});
        },
        "configuration closes when startup begins");
    rejects<std::logic_error>(
        [&] {
            runtime.set_source({.name = "late source", .run = [] {
                                }});
        },
        "source registration closes when startup begins");
    rejects<std::logic_error>(
        [&] {
            runtime.finish();
        },
        "source must finish before draining");
    runtime.run_source();
    rejects<std::logic_error>(
        [&] {
            runtime.run_source();
        },
        "source can run only once");
    runtime.finish();
    runtime.finish();
    rejects<std::logic_error>(
        [&] {
            runtime.start();
        },
        "joined pipelines cannot restart");
}

// Sources may have private waits; worker failure must also request source shutdown.
void worker_failure_wakes_a_registered_source() {
    WorkerError errors;
    BoundedQueue<int> input(1);
    std::mutex mutex;
    std::condition_variable cv;
    bool stop = false;
    std::atomic<int> stop_calls{0};
    std::atomic<bool> source_exited{false}, worker_exited{false};
    PipelineRuntime runtime(errors);
    runtime.set_source({
        .name = "waiting source",
        .run =
            [&] {
                check(input.push(1, &errors.cancelled), "source can submit its first task");
                std::unique_lock lock(mutex);
                check(cv.wait_for(lock, std::chrono::seconds(5),
                                  [&] {
                                      return stop;
                                  }),
                      "worker failure must wake the source's private wait");
                source_exited.store(true);
            },
        .request_stop =
            [&] {
                {
                    std::lock_guard lock(mutex);
                    stop = true;
                }
                ++stop_calls;
                cv.notify_all();
            },
    });
    runtime.add_stage({
        .name = "broken worker",
        .workers = 1,
        .run =
            [&](std::size_t) {
                if (input.pop(&errors.cancelled)) {
                    worker_exited.store(true);
                    throw std::runtime_error("registered worker failure");
                }
            },
        .close_input =
            [&] {
                input.close();
            },
    });

    std::string failure;
    try {
        runtime.run();
    } catch (const std::runtime_error& error) {
        failure = error.what();
    }
    check(failure == "registered worker failure", "registered source preserves the worker error");
    check(source_exited.load() && worker_exited.load(), "run returns only after both roles exit");
    runtime.cancel_and_join();
    check(stop_calls.load() == 1, "source cancellation callback runs once");
}

// run() joins before rethrowing, providing a stronger guarantee than destructor-only cleanup.
void run_joins_workers_before_rethrowing_source_failure() {
    WorkerError errors;
    BoundedQueue<int> input(1);
    std::atomic<bool> exited{false};
    PipelineRuntime runtime(errors);
    runtime.set_source({.name = "source", .run = [] {
                            throw std::runtime_error("source failure during run");
                        }});
    runtime.add_stage({
        .name = "consumer",
        .workers = 1,
        .run =
            [&](std::size_t) {
                while (input.pop(&errors.cancelled)) {
                }
                exited.store(true);
            },
        .close_input =
            [&] {
                input.close();
            },
    });

    std::string failure;
    try {
        runtime.run();
    } catch (const std::runtime_error& error) {
        failure = error.what();
    }
    check(failure == "source failure during run", "run preserves a source exception");
    check(exited.load(), "run joins workers before rethrowing, while the runtime is still alive");
}
} // namespace

int main() {
    try {
        stages_overlap_and_drain_in_order();
        worker_failure_unblocks_source_and_preserves_first_error();
        source_failure_cancels_waiting_workers();
        preparation_failure_cleans_up_without_starting_workers();
        observer_failure_cancels_the_pipeline();
        scope_exit_cancels_without_finish();
        configuration_and_lifecycle_are_checked();
        worker_failure_wakes_a_registered_source();
        run_joins_workers_before_rethrowing_source_failure();
        std::cout << "Pipeline runtime tests passed (9 cases)\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Pipeline runtime tests failed: " << error.what() << '\n';
        return 1;
    }
}
