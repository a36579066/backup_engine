cmake_minimum_required(VERSION 3.20)
project(paravault VERSION 0.1.0 LANGUAGES CXX)

option(PBE_BUILD_TESTS "Build the ParaVault unit and integration tests" ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)
find_package(SQLite3 REQUIRED)
find_path(ZSTD_INCLUDE_DIR zstd.h REQUIRED)
find_library(ZSTD_LIBRARY NAMES zstd REQUIRED)

add_library(pbe_pipeline_runtime
    src/pipeline/00_framework/pipeline_runtime.cpp
)
target_include_directories(pbe_pipeline_runtime PRIVATE src)
target_link_libraries(pbe_pipeline_runtime PUBLIC Threads::Threads)
target_compile_options(pbe_pipeline_runtime PRIVATE -Wall -Wextra -Wpedantic)

add_library(pbe_core
    src/benchmark/benchmark_runner.cpp
    src/core/engine.cpp
    src/core/backup_coordinator.cpp
    src/core/backup_session.cpp
    src/diagnostics/diagnostics.cpp
    src/diagnostics/pipeline_metrics.cpp
    src/diagnostics/pipeline_monitor.cpp
    src/hashing/blake3.cpp
    src/pipeline/01_reader/cached_file_reader.cpp
    src/pipeline/01_reader/metadata_batcher.cpp
    src/pipeline/01_reader/reader_stage.cpp
    src/pipeline/01_reader/source_file.cpp
    src/pipeline/01_reader/source_io_scheduler.cpp
    src/pipeline/01_reader/source_scanner.cpp
    src/pipeline/02_cpu/chunk_codec.cpp
    src/pipeline/02_cpu/chunk_completion.cpp
    src/pipeline/02_cpu/cpu_stage.cpp
    src/pipeline/02_cpu/object_reuse.cpp
    src/pipeline/03_storage/loose_writer.cpp
    src/pipeline/03_storage/pack_writer.cpp
    src/pipeline/04_metadata/metadata_commit.cpp
    src/pipeline/04_metadata/metadata_writer.cpp
    src/pipeline/backup_pipeline.cpp
    src/pipeline/shared/pipeline_context.cpp
    src/pipeline/shared/stage_support.cpp
    src/restore/restore_engine.cpp
    src/scheduler/adaptive_controller.cpp
    src/scheduler/io_policy.cpp
    src/storage/catalog.cpp
    src/storage/file_publication.cpp
    src/storage/object_store.cpp
    src/storage/repository.cpp
    src/storage/snapshot_catalog.cpp
)
target_include_directories(pbe_core PUBLIC include PRIVATE src ${ZSTD_INCLUDE_DIR})
target_link_libraries(pbe_core PUBLIC SQLite::SQLite3 Threads::Threads ${ZSTD_LIBRARY} PRIVATE pbe_pipeline_runtime)
target_compile_options(pbe_core PRIVATE -Wall -Wextra -Wpedantic)

add_executable(paravault src/cli/main.cpp)
target_link_libraries(paravault PRIVATE pbe_core)

# Keep the original command available for existing scripts and benchmark reports.
add_executable(pbe src/cli/main.cpp)
target_link_libraries(pbe PRIVATE pbe_core)

# Diagnostic tool only: reuse the production writer to measure an I/O reference.
# It is built explicitly and is not part of the installed application.
add_executable(pbe_io_floor EXCLUDE_FROM_ALL tools/benchmark_io_floor.cpp)
target_include_directories(pbe_io_floor PRIVATE src ${ZSTD_INCLUDE_DIR})
target_link_libraries(pbe_io_floor PRIVATE pbe_core)
target_compile_options(pbe_io_floor PRIVATE -Wall -Wextra -Wpedantic)

if(APPLE)
    # Independent POSIX I/O controls; no engine, hashing, SQLite, or Zstd dependency.
    add_executable(pbe_native_io EXCLUDE_FROM_ALL tools/benchmark_native_io.cpp)
    target_include_directories(pbe_native_io PRIVATE include)
    target_link_libraries(pbe_native_io PRIVATE Threads::Threads)
    target_compile_options(pbe_native_io PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(PBE_BUILD_TESTS)
    find_package(Python3 COMPONENTS Interpreter REQUIRED)
    enable_testing()
    add_executable(pbe_file_publication_tests tests/file_publication_tests.cpp)
    target_include_directories(pbe_file_publication_tests PRIVATE src)
    target_link_libraries(pbe_file_publication_tests PRIVATE pbe_core)
    add_test(NAME pbe_file_publication_tests COMMAND pbe_file_publication_tests)
    set_tests_properties(pbe_file_publication_tests PROPERTIES TIMEOUT 30)
    add_executable(pbe_pipeline_runtime_tests tests/pipeline_runtime_tests.cpp)
    target_include_directories(pbe_pipeline_runtime_tests PRIVATE src include)
    target_link_libraries(pbe_pipeline_runtime_tests PRIVATE pbe_pipeline_runtime)
    add_test(NAME pbe_pipeline_runtime_tests COMMAND pbe_pipeline_runtime_tests)
    set_tests_properties(pbe_pipeline_runtime_tests PROPERTIES TIMEOUT 30)
    add_executable(pbe_tests tests/pbe_tests.cpp)
    target_include_directories(pbe_tests PRIVATE src)
    target_link_libraries(pbe_tests PRIVATE pbe_core)
    add_test(NAME pbe_tests COMMAND pbe_tests)
    set_tests_properties(pbe_tests PROPERTIES TIMEOUT 180)
    add_executable(pbe_io_scheduler_tests tests/io_scheduler_tests.cpp)
    target_include_directories(pbe_io_scheduler_tests PRIVATE src)
    target_link_libraries(pbe_io_scheduler_tests PRIVATE pbe_core)
    add_test(NAME pbe_io_scheduler_tests COMMAND pbe_io_scheduler_tests)
    set_tests_properties(pbe_io_scheduler_tests PROPERTIES TIMEOUT 30)
    add_test(NAME pbe_cli_smoke
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/cli_smoke.py $<TARGET_FILE:paravault>)
    set_tests_properties(pbe_cli_smoke PROPERTIES TIMEOUT 60)
    add_test(NAME pbe_io_scheduling_cli
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/io_scheduling.py $<TARGET_FILE:paravault>)
    set_tests_properties(pbe_io_scheduling_cli PROPERTIES TIMEOUT 90)
endif()

install(TARGETS paravault pbe RUNTIME DESTINATION bin)
