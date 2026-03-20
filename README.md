# Distributed SQLite Database

A distributed SQL database built with MPI and SQLite. Data is hash-sharded across worker nodes by row ID, with a coordinator node (rank 0) routing queries and aggregating results.

## Architecture

```
                ┌─────────────┐
  SQL Input ──▶ │ Coordinator │ (MPI Rank 0)
                │  (rank 0)   │
                └──┬───┬───┬──┘
                   │   │   │   MPI messages
              ┌────┘   │   └────┐
              ▼        ▼        ▼
          ┌───────┐┌───────┐┌───────┐
          │Worker1││Worker2││Worker3│  ...
          │SQLite ││SQLite ││SQLite │
          └───────┘└───────┘└───────┘
```

- **Coordinator** - Parses SQL commands, determines target worker(s) using shard key (`id % num_workers`), dispatches via MPI, and merges results.
- **Workers** - Each maintains its own SQLite database file and executes operations locally.
- **Sharding** - Rows are distributed by the first integer value in `INSERT` or `WHERE id = N` clauses. DDL and queries without a shard key are broadcast to all workers.

## Supported Commands

| Command | Description |
|---|---|
| `CREATE DATABASE <name>` | Create a new database on all nodes |
| `DROP DATABASE <name>` | Drop a database from all nodes |
| `USE <name>` | Switch active database |
| `SHOW DATABASES` | List all databases |
| `SHOW TABLES` | List tables in current database |
| `CREATE TABLE ...` | Create a table (broadcast to all workers) |
| `DROP TABLE ...` | Drop a table (broadcast to all workers) |
| `INSERT INTO ...` | Insert a row (routed by shard key) |
| `SELECT ...` | Query rows (targeted or broadcast) |
| `UPDATE ...` | Update rows (targeted or broadcast) |
| `DELETE ...` | Delete rows (targeted or broadcast) |

## Building

Requires CMake 3.10+, a C++17 compiler, MPI, and SQLite3.

```bash
mkdir build && cd build
cmake ..
make
```

This produces three executables:
- `dist_db` - The main distributed database
- `gen_benchmark` - Generates benchmark SQL files
- `bench_single` - Single-node SQLite benchmark

## Usage

### Interactive mode

```bash
mpirun -np 4 ./dist_db
```

This starts 1 coordinator + 3 worker nodes. You'll get a `mysql>` prompt to enter commands.

### File mode

```bash
mpirun -np 4 ./dist_db --file commands.sql
```

Executes SQL statements from a file, then exits.

## Benchmarking

Generate benchmark data and run comparisons between distributed and single-node performance:

```bash
# Generate benchmark SQL files
./gen_benchmark 10000

# Run benchmarks (requires Python 3)
python tests/run_benchmarks.py

# Plot results
python tests/plot_results.py
```

## Project Structure

```
├── CMakeLists.txt
├── src/
│   ├── main.cpp             # Entry point, MPI init and rank dispatch
│   ├── Coordinator.cpp/hpp  # Command routing and result aggregation
│   ├── Worker.cpp/hpp       # Local SQL execution on each shard
│   ├── Database.cpp/hpp     # SQLite wrapper (read/write operations)
│   ├── CommandParser.cpp/hpp# SQL parsing and shard key extraction
│   └── Protocol.hpp         # MPI message tags and serialization
└── tests/
    ├── gen_benchmark.cpp    # SQL benchmark file generator
    ├── benchmark_single.cpp # Single-node performance test
    ├── run_benchmarks.py    # Benchmark orchestration
    └── plot_results.py      # Results visualization
```
