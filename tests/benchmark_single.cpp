// benchmark_single.cpp - Benchmarks single-node SQLite performance as a baseline
// for comparison against the distributed system. Reads SQL statements from a file,
// executes them on a local SQLite instance (in-memory or on-disk), and reports
// per-statement and total execution times. Supports optional transaction batching
// of consecutive write operations to mirror the distributed system's behavior.

#include <sqlite3.h>
#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

// Prints each row as comma-separated values; prints the column header on the
// first row. Returns the row count via the int* passed as user data.
static int select_callback(void* data, int argc, char** argv, char** col_names) {
    auto* state = static_cast<std::pair<bool, int>*>(data);
    bool& header_printed = state->first;
    int&  row_count       = state->second;

    if (!header_printed) {
        for (int i = 0; i < argc; i++) {
            std::cout << col_names[i];
            if (i < argc - 1) std::cout << ",";
        }
        std::cout << "\n";
        header_printed = true;
    }

    for (int i = 0; i < argc; i++) {
        std::cout << (argv[i] ? argv[i] : "");
        if (i < argc - 1) std::cout << ",";
    }
    std::cout << "\n";
    row_count++;
    return 0;
}

static std::string verb_of(const std::string& s) {
    std::string upper;
    upper.reserve(s.size());
    for (char c : s) upper += (char)toupper((unsigned char)c);
    size_t end = upper.find_first_of(" \t(");
    return upper.substr(0, end);
}

int main(int argc, char** argv) {
    // Usage: bench_single [sql_file] [--db <path>] [--txn]
    //   --db <path>  use a real file instead of :memory:
    //   --txn        wrap consecutive INSERTs in BEGIN/COMMIT (mirrors distributed system)
    std::string sql_file  = "tests/benchmark_single.sql";
    std::string db_path   = ":memory:";
    bool        use_txn   = false;

    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        if (a == "--db" && i + 1 < argc) { db_path = argv[++i]; }
        else if (a == "--txn")            { use_txn = true; }
        else                              { sql_file = a; }
    }

    std::cout << "DB: " << db_path
              << "  |  transactions: " << (use_txn ? "yes" : "no") << "\n";

    // --- open database ---
    sqlite3* db = nullptr;
    if (sqlite3_open(db_path.c_str(), &db) != SQLITE_OK) {
        std::cerr << "Failed to open database: " << sqlite3_errmsg(db) << "\n";
        return 1;
    }

    // --- read and parse SQL file ---
    std::ifstream file(sql_file);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open file: " << sql_file << "\n";
        return 1;
    }

    std::string content;
    std::string line;
    while (std::getline(file, line)) {
        size_t comment = line.find("--");
        if (comment != std::string::npos) line = line.substr(0, comment);
        content += line + " ";
    }

    std::vector<std::string> statements;
    std::istringstream stream(content);
    std::string stmt;
    while (std::getline(stream, stmt, ';')) {
        size_t first = stmt.find_first_not_of(" \t\n\r");
        if (first == std::string::npos) continue;
        size_t last = stmt.find_last_not_of(" \t\n\r");
        stmt = stmt.substr(first, last - first + 1);
        if (!stmt.empty()) statements.push_back(stmt);
    }

    // --- execute with optional INSERT batching ---
    long long total_ms = 0;
    int stmt_num = 0;
    size_t i = 0;

    while (i < statements.size()) {
        const std::string& s = statements[i];
        std::string v = verb_of(s);

        // Batch consecutive same-verb write statements under a single transaction
        // (mirrors the distributed system's batch optimizations for INSERT, DELETE, UPDATE)
        if (use_txn && (v == "INSERT" || v == "DELETE" || v == "UPDATE")) {
            std::string batch_verb = v;
            std::vector<std::string> batch;
            while (i < statements.size() && verb_of(statements[i]) == batch_verb)
                batch.push_back(statements[i++]);

            stmt_num++;
            std::cout << "\n[" << stmt_num << "] " << batch_verb << " batch (" << batch.size() << " statements)\n";

            auto t_start = std::chrono::high_resolution_clock::now();

            char* errmsg = nullptr;
            sqlite3_exec(db, "BEGIN", nullptr, nullptr, &errmsg);
            int success = 0;
            for (const auto& s_batch : batch) {
                if (sqlite3_exec(db, s_batch.c_str(), nullptr, nullptr, &errmsg) == SQLITE_OK)
                    success++;
                else
                    sqlite3_free(errmsg);
            }
            sqlite3_exec(db, "COMMIT", nullptr, nullptr, &errmsg);

            auto t_end = std::chrono::high_resolution_clock::now();
            long long ms = std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_start).count();
            total_ms += ms;

            std::cout << "Batch " << batch_verb << " OK, " << success << "/" << batch.size() << " statement(s).\n";
            std::cout << "-- " << ms << " ms\n";
            continue;
        }

        // Normal single statement
        stmt_num++;
        std::cout << "\n[" << stmt_num << "] " << s << "\n";

        char* errmsg = nullptr;
        auto t_start = std::chrono::high_resolution_clock::now();

        if (v == "SELECT") {
            std::pair<bool, int> state{false, 0};
            int rc = sqlite3_exec(db, s.c_str(), select_callback, &state, &errmsg);
            auto t_end = std::chrono::high_resolution_clock::now();
            long long ms = std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_start).count();
            total_ms += ms;
            if (rc != SQLITE_OK) { std::cerr << "Error: " << errmsg << "\n"; sqlite3_free(errmsg); }
            else std::cout << state.second << " row(s) in set\n";
            std::cout << "-- " << ms << " ms\n";
        } else {
            int rc = sqlite3_exec(db, s.c_str(), nullptr, nullptr, &errmsg);
            auto t_end = std::chrono::high_resolution_clock::now();
            long long ms = std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_start).count();
            total_ms += ms;
            if (rc != SQLITE_OK) { std::cerr << "Error: " << errmsg << "\n"; sqlite3_free(errmsg); }
            else if (v == "INSERT" || v == "UPDATE" || v == "DELETE")
                std::cout << "Query OK, " << sqlite3_changes(db) << " row(s) affected.\n";
            else
                std::cout << "Query OK, 0 rows affected.\n";
            std::cout << "-- " << ms << " ms\n";
        }
        i++;
    }

    sqlite3_close(db);
    if (db_path != ":memory:") std::remove(db_path.c_str());

    std::cout << "\n=== File complete: " << stmt_num << " statement(s), "
              << total_ms << " ms total ===\n";
    return 0;
}
