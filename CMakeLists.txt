cmake_minimum_required(VERSION 3.20)
project(parallel_backup_engine VERSION 0.1.0 LANGUAGES CXX)

option(PBE_BUILD_TESTS "Build the PBE unit and integration tests" ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)
find_package(SQLite3 REQUIRED)
find_path(ZSTD_INCLUDE_DIR zstd.h REQUIRED)
find_library(ZSTD_LIBRARY NAMES zstd REQUIRED)

add_library(pbe_core
    src/benchmark/benchmark_runner.cpp
    src/core/engine.cpp
    src/diagnostics/diagnostics.cpp
    src/hashing/blake3.cpp
    src/pipeline/metadata_stage.cpp
    src/restore/restore_engine.cpp
    src/scheduler/adaptive_controller.cpp
    src/storage/catalog.cpp
    src/storage/object_store.cpp
    src/storage/repository.cpp
    src/storage/snapshot_catalog.cpp
)
target_include_directories(pbe_core PUBLIC include PRIVATE src ${ZSTD_INCLUDE_DIR})
target_link_libraries(pbe_core PUBLIC SQLite::SQLite3 Threads::Threads ${ZSTD_LIBRARY})
target_compile_options(pbe_core PRIVATE -Wall -Wextra -Wpedantic)

add_executable(pbe src/cli/main.cpp)
target_link_libraries(pbe PRIVATE pbe_core)

if(PBE_BUILD_TESTS)
    find_package(Python3 COMPONENTS Interpreter REQUIRED)
    enable_testing()
    add_executable(pbe_tests tests/pbe_tests.cpp)
    target_include_directories(pbe_tests PRIVATE src)
    target_link_libraries(pbe_tests PRIVATE pbe_core)
    add_test(NAME pbe_tests COMMAND pbe_tests)
    set_tests_properties(pbe_tests PROPERTIES TIMEOUT 180)
    add_test(NAME pbe_cli_smoke
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/cli_smoke.py $<TARGET_FILE:pbe>)
    set_tests_properties(pbe_cli_smoke PROPERTIES TIMEOUT 60)
endif()

install(TARGETS pbe RUNTIME DESTINATION bin)
