cmake_minimum_required(VERSION 3.10)
project(DistributedSQL)

set(CMAKE_CXX_STANDARD 17)

# 1. Find Dependencies
find_package(MPI REQUIRED)
find_package(SQLite3 REQUIRED)

# 2. Define Source Files
set(SOURCES
    src/main.cpp
    src/Database.cpp
    src/Coordinator.cpp
    src/Worker.cpp
    src/CommandParser.cpp
)

# 3. Create Executable
add_executable(dist_db ${SOURCES})

# 4. Link Libraries
target_link_libraries(dist_db
    MPI::MPI_CXX
    SQLite::SQLite3
)

# 5. Include Directories
target_include_directories(dist_db PRIVATE src)

# 6. Single-node benchmark (no MPI)
add_executable(bench_single tests/benchmark_single.cpp)
target_link_libraries(bench_single SQLite::SQLite3)
target_include_directories(bench_single PRIVATE src)

# 7. Benchmark SQL file generator (no deps)
add_executable(gen_benchmark tests/gen_benchmark.cpp)