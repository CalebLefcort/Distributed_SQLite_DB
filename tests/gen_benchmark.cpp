// Generates benchmark SQL files at arbitrary row counts.
// Usage: gen_benchmark <row_count> [output_dir]
// Writes: <output_dir>/benchmark_<N>.sql        (distributed)
//         <output_dir>/benchmark_single_<N>.sql  (single-node)

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

static const char* NAMES[] = {
    "Alice","Bob","Carol","Dave","Eve","Frank","Grace","Hank","Ivy","Jack",
    "Karen","Leo","Mia","Ned","Olivia","Paul","Quinn","Rose","Sam","Tina",
    "Uma","Victor","Wendy","Xander","Yara","Zoe","Aaron","Bella","Chris","Diana"
};
static const int NAME_COUNT = 30;

static const char* DEPTS[] = {
    "Engineering","Marketing","HR","Sales","Finance"
};
static const int DEPT_COUNT = 5;

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: gen_benchmark <row_count> [output_dir]\n";
        return 1;
    }
    int n = std::atoi(argv[1]);
    if (n <= 0) { std::cerr << "row_count must be > 0\n"; return 1; }

    std::string dir = (argc >= 3) ? argv[2] : ".";

    std::string dist_path   = dir + "/benchmark_" + std::to_string(n) + ".sql";
    std::string single_path = dir + "/benchmark_single_" + std::to_string(n) + ".sql";

    std::ofstream dist(dist_path);
    std::ofstream single(single_path);

    if (!dist || !single) {
        std::cerr << "Failed to open output files in: " << dir << "\n";
        return 1;
    }

    // ── headers ──────────────────────────────────────────────────────────────
    dist   << "-- Distributed benchmark: " << n << " rows\n"
           << "-- Run: mpirun -n 4 ./dist_db --file " << dist_path << "\n\n";
    single << "-- Single-node benchmark: " << n << " rows\n"
           << "-- Run: ./bench_single " << single_path << "\n\n";

    const char* DDL = "CREATE TABLE employees "
                      "(id INTEGER PRIMARY KEY, name TEXT, department TEXT, salary REAL)";

    dist   << "CREATE DATABASE bench;\nUSE bench;\n" << DDL << ";\n\n";
    single << DDL << ";\n\n";

    // ── inserts ──────────────────────────────────────────────────────────────
    dist   << "-- " << n << " inserts\n";
    single << "-- " << n << " inserts\n";

    for (int i = 1; i <= n; i++) {
        const char* name = NAMES[i % NAME_COUNT];
        const char* dept = DEPTS[i % DEPT_COUNT];
        int salary = 50000 + (i * 137) % 100000;
        std::string row = "INSERT INTO employees VALUES (" + std::to_string(i)
            + ", '" + name + "_" + std::to_string(i)
            + "', '" + dept + "', " + std::to_string(salary) + ");\n";
        dist   << row;
        single << row;
    }

    // ── targeted selects ─────────────────────────────────────────────────────
    int mid = n / 2;
    dist   << "\n-- Targeted selects\n";
    single << "\n-- Targeted selects\n";
    for (int id : {1, mid, n}) {
        std::string q = "SELECT * FROM employees WHERE id = " + std::to_string(id) + ";\n";
        dist   << q;
        single << q;
    }

    // ── full scan ─────────────────────────────────────────────────────────────
    dist   << "\n-- Full scan\nSELECT * FROM employees;\n";
    single << "\n-- Full scan\nSELECT * FROM employees;\n";

    // ── broadcast update ─────────────────────────────────────────────────────
    dist   << "\n-- Broadcast update\n"
           << "UPDATE employees SET salary = salary * 1.10 WHERE department = 'Engineering';\n";
    single << "\n-- Broadcast update\n"
           << "UPDATE employees SET salary = salary * 1.10 WHERE department = 'Engineering';\n";

    // ── targeted deletes ──────────────────────────────────────────────────────
    dist   << "\n-- Targeted deletes\n";
    single << "\n-- Targeted deletes\n";
    for (int id : {1, mid, n}) {
        std::string q = "DELETE FROM employees WHERE id = " + std::to_string(id) + ";\n";
        dist   << q;
        single << q;
    }

    // ── final count via full scan ─────────────────────────────────────────────
    dist   << "\n-- Final state\nSELECT * FROM employees;\n";
    single << "\n-- Final state\nSELECT * FROM employees;\n";

    // ── teardown ──────────────────────────────────────────────────────────────
    dist   << "\n-- Teardown\nDROP TABLE employees;\nDROP DATABASE bench;\n";
    single << "\n-- Teardown\nDROP TABLE employees;\n";

    std::cout << "Generated:\n  " << dist_path << "\n  " << single_path << "\n";
    return 0;
}
