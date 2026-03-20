// Coordinator.cpp - Runs on MPI rank 0 and acts as the central command router.
// Accepts SQL commands (interactively or from a file), parses them, and dispatches
// work to worker nodes via MPI. Handles database/table management, shard-key-based
// routing for DML operations, and aggregation of SELECT results from all workers.

#include "Coordinator.hpp"
#include "Protocol.hpp"
#include "CommandParser.hpp"
#include <mpi.h>
#include <iostream>
#include <string>
#include <vector>
#include <set>
#include <cstring>
#include <iomanip>
#include <thread>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>

// Helper: serialize a DatabaseMessage and send via MPI
static void send_db_msg(const DatabaseMessage& msg, int dest, int tag) {
    auto buf = msg.serialize();
    MPI_Send(buf.data(), static_cast<int>(buf.size()), MPI_BYTE, dest, tag, MPI_COMM_WORLD);
}

// Forward declarations
void handle_create_database(const std::string& db_name, std::set<std::string>& databases, int world_size);
void handle_drop_database(const std::string& db_name, std::set<std::string>& databases,
                          std::string& current_database, int world_size);
void handle_use_database(const std::string& db_name, const std::set<std::string>& databases,
                        std::string& current_database, int world_size);
void handle_show_databases(const std::set<std::string>& databases);
void handle_show_tables(const std::string& current_database);
void handle_table_ddl(const std::string& sql, const std::string& current_database, int world_size);
void handle_insert(const std::string& sql, int shard_key, const std::string& current_database, int world_size);
void handle_select(const std::string& sql, int shard_key, const std::string& current_database, int world_size);
void handle_update(const std::string& sql, int shard_key, const std::string& current_database, int world_size);
void handle_delete(const std::string& sql, int shard_key, const std::string& current_database, int world_size);
void shutdown_workers(int world_size);

// Scan working directory for existing *_node1.db files to restore the database list.
static std::set<std::string> discover_databases() {
    std::set<std::string> found;
    const std::string suffix = "_node1.db";
    for (const auto& entry : std::filesystem::directory_iterator(".")) {
        if (!entry.is_regular_file()) continue;
        std::string name = entry.path().filename().string();
        if (name.size() > suffix.size() &&
            name.compare(name.size() - suffix.size(), suffix.size(), suffix) == 0) {
            found.insert(name.substr(0, name.size() - suffix.size()));
        }
    }
    return found;
}

// Dispatches a parsed command. Returns false if EXIT was received.
static bool dispatch_command(const CommandParser::ParsedCommand& cmd,
                             std::string& current_database,
                             std::set<std::string>& databases,
                             int world_size) {
    switch (cmd.type) {
        case CommandParser::CommandType::EXIT:
            return false;
        case CommandParser::CommandType::CREATE_DATABASE:
            handle_create_database(cmd.database_name, databases, world_size);
            break;
        case CommandParser::CommandType::USE_DATABASE:
            handle_use_database(cmd.database_name, databases, current_database, world_size);
            break;
        case CommandParser::CommandType::SHOW_DATABASES:
            handle_show_databases(databases);
            break;
        case CommandParser::CommandType::SHOW_TABLES:
            handle_show_tables(current_database);
            break;
        case CommandParser::CommandType::DROP_DATABASE:
            handle_drop_database(cmd.database_name, databases, current_database, world_size);
            break;
        case CommandParser::CommandType::CREATE_TABLE:
        case CommandParser::CommandType::DROP_TABLE:
            handle_table_ddl(cmd.sql, current_database, world_size);
            break;
        case CommandParser::CommandType::INSERT:
            handle_insert(cmd.sql, cmd.shard_key, current_database, world_size);
            break;
        case CommandParser::CommandType::SELECT:
            handle_select(cmd.sql, cmd.shard_key, current_database, world_size);
            break;
        case CommandParser::CommandType::UPDATE:
            handle_update(cmd.sql, cmd.shard_key, current_database, world_size);
            break;
        case CommandParser::CommandType::DELETE_ROW:
            handle_delete(cmd.sql, cmd.shard_key, current_database, world_size);
            break;
        default:
            std::cerr << "Error: Unknown command type.\n";
            break;
    }
    return true;
}

// Sends a batch of targeted INSERTs in a single fan-out (no per-INSERT ACK wait).
// All messages are sent first; then all ACKs are collected.
// Broadcasts a single DDL statement (e.g. "BEGIN" / "COMMIT") to every worker
// via TAG_TABLE_DDL and waits for all ACKs before returning.
static void broadcast_ddl(const char* sql, const std::string& current_database, int world_size) {
    DatabaseMessage msg;
    strncpy(msg.db_name, current_database.c_str(), sizeof(msg.db_name) - 1);
    msg.db_name[sizeof(msg.db_name) - 1] = '\0';
    strncpy(msg.sql_query, sql, sizeof(msg.sql_query) - 1);
    msg.sql_query[sizeof(msg.sql_query) - 1] = '\0';
    for (int i = 1; i < world_size; i++)
        send_db_msg(msg, i, TAG_TABLE_DDL);
    for (int i = 1; i < world_size; i++) {
        int ack; MPI_Status st;
        MPI_Recv(&ack, 1, MPI_INT, MPI_ANY_SOURCE, TAG_ACK, MPI_COMM_WORLD, &st);
    }
}

// Sends a batch of targeted INSERTs wrapped in a single BEGIN/COMMIT transaction
// on each worker, eliminating per-INSERT fsync overhead.
static void handle_insert_batch(const std::vector<std::pair<std::string, int>>& batch,
                                 const std::string& current_database, int world_size) {
    DatabaseMessage msg;
    strncpy(msg.db_name, current_database.c_str(), sizeof(msg.db_name) - 1);
    msg.db_name[sizeof(msg.db_name) - 1] = '\0';

    // Open a transaction on every worker (1 fsync at COMMIT instead of N fsyncs)
    broadcast_ddl("BEGIN", current_database, world_size);

    // Fire all INSERTs without waiting for per-INSERT ACKs
    for (const auto& [sql, shard_key] : batch) {
        strncpy(msg.sql_query, sql.c_str(), sizeof(msg.sql_query) - 1);
        msg.sql_query[sizeof(msg.sql_query) - 1] = '\0';
        int target = get_target_worker(shard_key, world_size);
        send_db_msg(msg, target, TAG_INSERT);
    }

    // Collect all INSERT ACKs
    int success = 0;
    for (size_t i = 0; i < batch.size(); i++) {
        int ack; MPI_Status st;
        MPI_Recv(&ack, 1, MPI_INT, MPI_ANY_SOURCE, TAG_ACK, MPI_COMM_WORLD, &st);
        if (ack == 0) success++;
    }

    // Commit — each worker does exactly 1 fsync for the entire batch
    broadcast_ddl("COMMIT", current_database, world_size);

    std::cout << "Batch INSERT OK, " << success << "/" << batch.size() << " row(s) inserted.\n";
}

// Sends a batch of targeted DELETEs wrapped in a single BEGIN/COMMIT transaction
// on each worker, eliminating per-DELETE fsync overhead.
static void handle_delete_batch(const std::vector<std::pair<std::string, int>>& batch,
                                 const std::string& current_database, int world_size) {
    DatabaseMessage msg;
    strncpy(msg.db_name, current_database.c_str(), sizeof(msg.db_name) - 1);
    msg.db_name[sizeof(msg.db_name) - 1] = '\0';

    broadcast_ddl("BEGIN", current_database, world_size);

    for (const auto& [sql, shard_key] : batch) {
        strncpy(msg.sql_query, sql.c_str(), sizeof(msg.sql_query) - 1);
        msg.sql_query[sizeof(msg.sql_query) - 1] = '\0';
        int target = get_target_worker(shard_key, world_size);
        send_db_msg(msg, target, TAG_DELETE);
    }

    int success = 0;
    for (size_t i = 0; i < batch.size(); i++) {
        int ack; MPI_Status st;
        MPI_Recv(&ack, 1, MPI_INT, MPI_ANY_SOURCE, TAG_ACK, MPI_COMM_WORLD, &st);
        if (ack == 0) success++;
    }

    broadcast_ddl("COMMIT", current_database, world_size);

    std::cout << "Batch DELETE OK, " << success << "/" << batch.size() << " row(s) deleted.\n";
}

// Sends a batch of UPDATEs (targeted or broadcast) wrapped in BEGIN/COMMIT.
// Targeted UPDATEs go to specific workers; broadcast UPDATEs go to all.
static void handle_update_batch(const std::vector<std::pair<std::string, int>>& batch,
                                 const std::string& current_database, int world_size) {
    DatabaseMessage msg;
    strncpy(msg.db_name, current_database.c_str(), sizeof(msg.db_name) - 1);
    msg.db_name[sizeof(msg.db_name) - 1] = '\0';

    broadcast_ddl("BEGIN", current_database, world_size);

    int expected_acks = 0;
    for (const auto& [sql, shard_key] : batch) {
        strncpy(msg.sql_query, sql.c_str(), sizeof(msg.sql_query) - 1);
        msg.sql_query[sizeof(msg.sql_query) - 1] = '\0';
        if (shard_key != -1) {
            int target = get_target_worker(shard_key, world_size);
            send_db_msg(msg, target, TAG_UPDATE);
            expected_acks++;
        } else {
            for (int w = 1; w < world_size; w++)
                send_db_msg(msg, w, TAG_UPDATE);
            expected_acks += world_size - 1;
        }
    }

    int success = 0;
    for (int i = 0; i < expected_acks; i++) {
        int ack; MPI_Status st;
        MPI_Recv(&ack, 1, MPI_INT, MPI_ANY_SOURCE, TAG_ACK, MPI_COMM_WORLD, &st);
        if (ack == 0) success++;
    }

    broadcast_ddl("COMMIT", current_database, world_size);

    std::cout << "Batch UPDATE OK, " << success << "/" << expected_acks << " ack(s) received.\n";
}

// Reads an SQL file, strips -- comments, splits on ;, and executes each
// statement with per-statement timing. Consecutive targeted INSERTs are
// pipelined as a single batch (send-all then collect-all) for speed.
static void run_sql_file(const std::string& path,
                         std::string& current_database,
                         std::set<std::string>& databases,
                         int world_size) {
    std::ifstream file(path);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open file: " << path << "\n";
        return;
    }

    // Read file, stripping -- line comments
    std::string content;
    std::string line;
    while (std::getline(file, line)) {
        size_t comment = line.find("--");
        if (comment != std::string::npos) line = line.substr(0, comment);
        content += line + " ";
    }

    // Split on ; into a vector for look-ahead batching
    std::vector<std::string> stmts;
    std::istringstream stream(content);
    std::string stmt;
    while (std::getline(stream, stmt, ';')) {
        size_t first = stmt.find_first_not_of(" \t\n\r");
        if (first == std::string::npos) continue;
        size_t last = stmt.find_last_not_of(" \t\n\r");
        stmt = stmt.substr(first, last - first + 1);
        if (!stmt.empty()) stmts.push_back(stmt);
    }

    int stmt_num = 0;
    long long total_ms = 0;
    size_t i = 0;

    while (i < stmts.size()) {
        auto cmd = CommandParser::parse(stmts[i]);

        // Batch consecutive targeted INSERTs into a single pipelined send
        if (cmd.is_valid &&
            cmd.type == CommandParser::CommandType::INSERT &&
            cmd.shard_key != -1 &&
            !current_database.empty()) {

            std::vector<std::pair<std::string, int>> batch;
            while (i < stmts.size()) {
                auto c = CommandParser::parse(stmts[i]);
                if (!c.is_valid ||
                    c.type != CommandParser::CommandType::INSERT ||
                    c.shard_key == -1) break;
                batch.emplace_back(c.sql, c.shard_key);
                i++;
            }

            stmt_num++;
            std::cout << "\n[" << stmt_num << "] INSERT batch ("
                      << batch.size() << " rows)\n";
            auto t_start = std::chrono::high_resolution_clock::now();
            handle_insert_batch(batch, current_database, world_size);
            auto t_end = std::chrono::high_resolution_clock::now();
            long long ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               t_end - t_start).count();
            total_ms += ms;
            std::cout << "-- " << ms << " ms\n" << std::flush;
            continue;
        }

        // Batch consecutive UPDATEs into a single pipelined send
        if (cmd.is_valid &&
            cmd.type == CommandParser::CommandType::UPDATE &&
            !current_database.empty()) {

            std::vector<std::pair<std::string, int>> batch;
            while (i < stmts.size()) {
                auto c = CommandParser::parse(stmts[i]);
                if (!c.is_valid ||
                    c.type != CommandParser::CommandType::UPDATE) break;
                batch.emplace_back(c.sql, c.shard_key);
                i++;
            }

            stmt_num++;
            std::cout << "\n[" << stmt_num << "] UPDATE batch ("
                      << batch.size() << " statements)\n";
            auto t_start = std::chrono::high_resolution_clock::now();
            handle_update_batch(batch, current_database, world_size);
            auto t_end = std::chrono::high_resolution_clock::now();
            long long ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               t_end - t_start).count();
            total_ms += ms;
            std::cout << "-- " << ms << " ms\n" << std::flush;
            continue;
        }

        // Batch consecutive targeted DELETEs into a single pipelined send
        if (cmd.is_valid &&
            cmd.type == CommandParser::CommandType::DELETE_ROW &&
            cmd.shard_key != -1 &&
            !current_database.empty()) {

            std::vector<std::pair<std::string, int>> batch;
            while (i < stmts.size()) {
                auto c = CommandParser::parse(stmts[i]);
                if (!c.is_valid ||
                    c.type != CommandParser::CommandType::DELETE_ROW ||
                    c.shard_key == -1) break;
                batch.emplace_back(c.sql, c.shard_key);
                i++;
            }

            stmt_num++;
            std::cout << "\n[" << stmt_num << "] DELETE batch ("
                      << batch.size() << " rows)\n";
            auto t_start = std::chrono::high_resolution_clock::now();
            handle_delete_batch(batch, current_database, world_size);
            auto t_end = std::chrono::high_resolution_clock::now();
            long long ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               t_end - t_start).count();
            total_ms += ms;
            std::cout << "-- " << ms << " ms\n" << std::flush;
            continue;
        }

        // Normal single statement
        stmt_num++;
        std::cout << "\n[" << stmt_num << "] " << stmts[i] << "\n";

        if (!cmd.is_valid) {
            std::cerr << "Error: " << cmd.error_message << "\n";
            i++;
            continue;
        }

        auto t_start = std::chrono::high_resolution_clock::now();
        bool keep_going = dispatch_command(cmd, current_database, databases, world_size);
        auto t_end = std::chrono::high_resolution_clock::now();

        long long ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                           t_end - t_start).count();
        total_ms += ms;
        std::cout << "-- " << ms << " ms\n" << std::flush;
        i++;

        if (!keep_going) break;
    }

    std::cout << "\n=== File complete: " << stmt_num << " statement(s), "
              << total_ms << " ms total ===\n" << std::flush;
}

void run_coordinator(int world_size, const std::string& sql_file) {
    std::cout << "=== Distributed SQL Database (Coordinator) ===\n";
    std::cout << "Nodes active: " << world_size << " (1 Coordinator + "
              << world_size-1 << " Workers)\n";
    std::cout.flush();

    // Give workers a moment to print their initialization messages
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::string current_database = "";
    std::set<std::string> databases = discover_databases();
    if (!databases.empty()) {
        std::cout << "Restored " << databases.size() << " existing database(s).\n";
    }

    if (!sql_file.empty()) {
        run_sql_file(sql_file, current_database, databases, world_size);
        shutdown_workers(world_size);
        return;
    }

    std::cout << "Type 'exit' or 'quit' to quit.\n\n" << std::flush;

    while (true) {
        if (current_database.empty()) {
            std::cout << "mysql> " << std::flush;
        } else {
            std::cout << "mysql [" << current_database << "]> " << std::flush;
        }

        std::string input;
        if (!std::getline(std::cin, input)) break;
        if (input.empty()) continue;

        auto cmd = CommandParser::parse(input);
        if (!cmd.is_valid) {
            std::cerr << "Error: " << cmd.error_message << "\n";
            continue;
        }

        if (!dispatch_command(cmd, current_database, databases, world_size)) {
            shutdown_workers(world_size);
            std::cout << "Bye\n";
            return;
        }
    }
}

void handle_create_database(const std::string& db_name, std::set<std::string>& databases, int world_size) {
    if (databases.find(db_name) != databases.end()) {
        std::cout << "Database '" << db_name << "' already exists.\n";
        return;
    }

    DatabaseMessage msg;
    strncpy(msg.db_name, db_name.c_str(), sizeof(msg.db_name) - 1);
    msg.db_name[sizeof(msg.db_name) - 1] = '\0';

    for (int i = 1; i < world_size; i++)
        send_db_msg(msg, i, TAG_CREATE_DB);

    int success_count = 0;
    for (int i = 1; i < world_size; i++) {
        int ack_status;
        MPI_Status status;
        MPI_Recv(&ack_status, 1, MPI_INT, MPI_ANY_SOURCE, TAG_ACK, MPI_COMM_WORLD, &status);
        if (ack_status == 0) success_count++;
    }

    if (success_count == world_size - 1) {
        databases.insert(db_name);
        std::cout << "Database '" << db_name << "' created successfully.\n";
    } else {
        std::cout << "Error: Failed to create database on all workers.\n";
    }
}

void handle_drop_database(const std::string& db_name, std::set<std::string>& databases,
                         std::string& current_database, int world_size) {
    if (databases.find(db_name) == databases.end()) {
        std::cout << "Error: Database '" << db_name << "' does not exist.\n";
        return;
    }

    DatabaseMessage msg;
    strncpy(msg.db_name, db_name.c_str(), sizeof(msg.db_name) - 1);
    msg.db_name[sizeof(msg.db_name) - 1] = '\0';

    for (int i = 1; i < world_size; i++)
        send_db_msg(msg, i, TAG_DROP_DB);

    int success_count = 0;
    for (int i = 1; i < world_size; i++) {
        int ack_status;
        MPI_Status status;
        MPI_Recv(&ack_status, 1, MPI_INT, MPI_ANY_SOURCE, TAG_ACK, MPI_COMM_WORLD, &status);
        if (ack_status == 0) success_count++;
    }

    if (success_count == world_size - 1) {
        databases.erase(db_name);
        if (current_database == db_name) {
            current_database = "";
        }
        std::cout << "Database '" << db_name << "' dropped successfully.\n";
    } else {
        std::cout << "Error: Failed to drop database on all workers.\n";
    }
}

void handle_use_database(const std::string& db_name, const std::set<std::string>& databases,
                        std::string& current_database, int world_size) {
    if (databases.find(db_name) == databases.end()) {
        std::cout << "Error: Database '" << db_name << "' does not exist.\n";
        return;
    }

    DatabaseMessage msg;
    strncpy(msg.db_name, db_name.c_str(), sizeof(msg.db_name) - 1);
    msg.db_name[sizeof(msg.db_name) - 1] = '\0';

    for (int i = 1; i < world_size; i++)
        send_db_msg(msg, i, TAG_USE_DB);

    int success_count = 0;
    for (int i = 1; i < world_size; i++) {
        int ack_status;
        MPI_Status status;
        MPI_Recv(&ack_status, 1, MPI_INT, MPI_ANY_SOURCE, TAG_ACK, MPI_COMM_WORLD, &status);
        if (ack_status == 0) success_count++;
    }

    if (success_count == world_size - 1) {
        current_database = db_name;
        std::cout << "Database changed to '" << db_name << "'.\n";
    } else {
        std::cout << "Error: Failed to switch database on all workers.\n";
    }
}

void handle_show_databases(const std::set<std::string>& databases) {
    std::cout << "+--------------------+\n";
    std::cout << "| Database           |\n";
    std::cout << "+--------------------+\n";
    for (const auto& db : databases) {
        std::cout << "| " << std::left << std::setw(18) << db << " |\n";
    }
    std::cout << "+--------------------+\n";
    std::cout << databases.size() << " database(s)\n";
}

void handle_show_tables(const std::string& current_database) {
    if (current_database.empty()) {
        std::cout << "Error: No database selected. Use 'USE <database>' first.\n";
        return;
    }

    // All workers share the same schema — query worker 1 only
    DatabaseMessage msg;
    strncpy(msg.db_name, current_database.c_str(), sizeof(msg.db_name) - 1);
    msg.db_name[sizeof(msg.db_name) - 1] = '\0';
    strncpy(msg.sql_query, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
            sizeof(msg.sql_query) - 1);
    msg.sql_query[sizeof(msg.sql_query) - 1] = '\0';
    msg.needs_response = 1;

    send_db_msg(msg, 1, TAG_SELECT);

    ResultMessage res_msg;
    MPI_Recv(&res_msg, sizeof(ResultMessage), MPI_BYTE, 1, TAG_RESULT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    if (res_msg.status != 0) {
        std::cerr << "Error: " << res_msg.error_msg << "\n";
        return;
    }

    // Phase 2: receive variable-length data if there are rows
    std::string data;
    if (res_msg.row_count > 0) {
        MPI_Status data_status;
        MPI_Probe(1, TAG_RESULT_DATA, MPI_COMM_WORLD, &data_status);
        int data_count;
        MPI_Get_count(&data_status, MPI_CHAR, &data_count);
        data.resize(data_count);
        MPI_Recv(&data[0], data_count, MPI_CHAR, 1, TAG_RESULT_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }

    std::cout << "+--------------------+\n";
    std::cout << "| Tables             |\n";
    std::cout << "+--------------------+\n";

    // Result is "name\n<row>\n..." — skip the header line
    size_t pos = data.find('\n');  // skip "name\n" header
    while (pos != std::string::npos && pos + 1 < data.size()) {
        size_t next = data.find('\n', pos + 1);
        std::string table = data.substr(pos + 1, next - pos - 1);
        if (!table.empty()) {
            std::cout << "| " << std::left << std::setw(18) << table << " |\n";
        }
        pos = next;
    }

    std::cout << "+--------------------+\n";
    std::cout << res_msg.row_count << " table(s)\n";
}

void handle_table_ddl(const std::string& sql, const std::string& current_database, int world_size) {
    if (current_database.empty()) {
        std::cout << "Error: No database selected. Use 'USE <database>' first.\n";
        return;
    }

    DatabaseMessage msg;
    strncpy(msg.db_name, current_database.c_str(), sizeof(msg.db_name) - 1);
    msg.db_name[sizeof(msg.db_name) - 1] = '\0';
    strncpy(msg.sql_query, sql.c_str(), sizeof(msg.sql_query) - 1);
    msg.sql_query[sizeof(msg.sql_query) - 1] = '\0';

    for (int i = 1; i < world_size; i++)
        send_db_msg(msg, i, TAG_TABLE_DDL);

    int success_count = 0;
    for (int i = 1; i < world_size; i++) {
        int ack_status;
        MPI_Status status;
        MPI_Recv(&ack_status, 1, MPI_INT, MPI_ANY_SOURCE, TAG_ACK, MPI_COMM_WORLD, &status);
        if (ack_status == 0) success_count++;
    }

    if (success_count == world_size - 1) {
        std::cout << "Query OK, 0 rows affected.\n";
    } else {
        std::cout << "Error: Failed to create table on all workers.\n";
    }
}

void handle_insert(const std::string& sql, int shard_key, const std::string& current_database, int world_size) {
    if (current_database.empty()) {
        std::cout << "Error: No database selected. Use 'USE <database>' first.\n";
        return;
    }

    DatabaseMessage msg;
    strncpy(msg.db_name, current_database.c_str(), sizeof(msg.db_name) - 1);
    msg.db_name[sizeof(msg.db_name) - 1] = '\0';
    strncpy(msg.sql_query, sql.c_str(), sizeof(msg.sql_query) - 1);
    msg.sql_query[sizeof(msg.sql_query) - 1] = '\0';

    if (shard_key != -1) {
        // Send to specific worker
        int target = get_target_worker(shard_key, world_size);
        send_db_msg(msg, target, TAG_INSERT);

        int ack_status;
        MPI_Status status;
        MPI_Recv(&ack_status, 1, MPI_INT, target, TAG_ACK, MPI_COMM_WORLD, &status);

        if (ack_status == 0) {
            std::cout << "Query OK, 1 row affected.\n";
        } else {
            std::cout << "Error: Insert failed.\n";
        }
    } else {
        // Broadcast to all workers (no shard key found)
        for (int i = 1; i < world_size; i++)
            send_db_msg(msg, i, TAG_INSERT);

        int success_count = 0;
        for (int i = 1; i < world_size; i++) {
            int ack_status;
            MPI_Status status;
            MPI_Recv(&ack_status, 1, MPI_INT, MPI_ANY_SOURCE, TAG_ACK, MPI_COMM_WORLD, &status);
            if (ack_status == 0) success_count++;
        }

        std::cout << "Query OK, " << success_count << " row(s) affected.\n";
    }
}

void handle_select(const std::string& sql, int shard_key, const std::string& current_database, int world_size) {
    if (current_database.empty()) {
        std::cout << "Error: No database selected. Use 'USE <database>' first.\n";
        return;
    }

    DatabaseMessage msg;
    strncpy(msg.db_name, current_database.c_str(), sizeof(msg.db_name) - 1);
    msg.db_name[sizeof(msg.db_name) - 1] = '\0';
    strncpy(msg.sql_query, sql.c_str(), sizeof(msg.sql_query) - 1);
    msg.sql_query[sizeof(msg.sql_query) - 1] = '\0';
    msg.needs_response = 1;

    std::vector<int> workers_to_query;
    if (shard_key != -1) {
        // Query specific worker only
        workers_to_query.push_back(get_target_worker(shard_key, world_size));
    } else {
        // Query all workers
        for (int i = 1; i < world_size; i++) {
            workers_to_query.push_back(i);
        }
    }

    // Send SELECT to workers
    for (int worker : workers_to_query) {
        send_db_msg(msg, worker, TAG_SELECT);
    }

    // Collect all result headers first, then fetch data — allows workers to
    // send concurrently instead of blocking on the coordinator's serial recv.
    std::vector<ResultMessage> headers(workers_to_query.size());
    std::vector<int> workers_with_data;   // indices into headers[]
    int total_rows = 0;
    bool has_errors = false;

    // Phase 1: receive all fixed-size headers (any order)
    for (size_t i = 0; i < workers_to_query.size(); i++) {
        MPI_Recv(&headers[i], sizeof(ResultMessage), MPI_BYTE, MPI_ANY_SOURCE,
                 TAG_RESULT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if (headers[i].status != 0) {
            has_errors = true;
            std::cerr << "Error from Worker " << headers[i].worker_rank
                     << ": " << headers[i].error_msg << "\n";
        } else {
            total_rows += headers[i].row_count;
            if (headers[i].row_count > 0)
                workers_with_data.push_back(i);
        }
    }

    // Phase 2: receive all variable-length result data
    std::vector<std::pair<ResultMessage, std::string>> results;
    for (int idx : workers_with_data) {
        MPI_Status data_status;
        MPI_Probe(headers[idx].worker_rank, TAG_RESULT_DATA, MPI_COMM_WORLD, &data_status);
        int data_count;
        MPI_Get_count(&data_status, MPI_CHAR, &data_count);
        std::string data(data_count, '\0');
        MPI_Recv(&data[0], data_count, MPI_CHAR, headers[idx].worker_rank,
                 TAG_RESULT_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        results.emplace_back(headers[idx], std::move(data));
    }

    // Display results — print header from first non-empty result only, then data from all
    bool header_printed = false;
    for (const auto& [hdr, data] : results) {
        if (data.empty()) continue;

        if (!header_printed) {
            std::cout << data;
            header_printed = true;
        } else {
            // Skip the header line (first line) from subsequent workers
            size_t newline_pos = data.find('\n');
            if (newline_pos != std::string::npos && newline_pos + 1 < data.size()) {
                std::cout << data.substr(newline_pos + 1);
            }
        }
    }

    std::cout << total_rows << " row(s) in set\n";
}

void handle_update(const std::string& sql, int shard_key, const std::string& current_database, int world_size) {
    if (current_database.empty()) {
        std::cout << "Error: No database selected. Use 'USE <database>' first.\n";
        return;
    }

    DatabaseMessage msg;
    strncpy(msg.db_name, current_database.c_str(), sizeof(msg.db_name) - 1);
    msg.db_name[sizeof(msg.db_name) - 1] = '\0';
    strncpy(msg.sql_query, sql.c_str(), sizeof(msg.sql_query) - 1);
    msg.sql_query[sizeof(msg.sql_query) - 1] = '\0';

    if (shard_key != -1) {
        // Send to specific worker
        int target = get_target_worker(shard_key, world_size);
        send_db_msg(msg, target, TAG_UPDATE);

        int ack_status;
        MPI_Status status;
        MPI_Recv(&ack_status, 1, MPI_INT, target, TAG_ACK, MPI_COMM_WORLD, &status);

        if (ack_status == 0) {
            std::cout << "Query OK, 1 row affected.\n";
        } else {
            std::cout << "Error: Update failed.\n";
        }
    } else {
        // Broadcast to all workers
        for (int i = 1; i < world_size; i++)
            MPI_Send(&msg, sizeof(DatabaseMessage), MPI_BYTE, i, TAG_UPDATE, MPI_COMM_WORLD);

        int success_count = 0;
        for (int i = 1; i < world_size; i++) {
            int ack_status;
            MPI_Status status;
            MPI_Recv(&ack_status, 1, MPI_INT, MPI_ANY_SOURCE, TAG_ACK, MPI_COMM_WORLD, &status);
            if (ack_status == 0) success_count++;
        }

        std::cout << "Query OK, " << success_count << " row(s) affected.\n";
    }
}

void handle_delete(const std::string& sql, int shard_key, const std::string& current_database, int world_size) {
    if (current_database.empty()) {
        std::cout << "Error: No database selected. Use 'USE <database>' first.\n";
        return;
    }

    DatabaseMessage msg;
    strncpy(msg.db_name, current_database.c_str(), sizeof(msg.db_name) - 1);
    msg.db_name[sizeof(msg.db_name) - 1] = '\0';
    strncpy(msg.sql_query, sql.c_str(), sizeof(msg.sql_query) - 1);
    msg.sql_query[sizeof(msg.sql_query) - 1] = '\0';

    if (shard_key != -1) {
        // Send to specific worker
        int target = get_target_worker(shard_key, world_size);
        send_db_msg(msg, target, TAG_DELETE);

        int ack_status;
        MPI_Status status;
        MPI_Recv(&ack_status, 1, MPI_INT, target, TAG_ACK, MPI_COMM_WORLD, &status);

        if (ack_status == 0) {
            std::cout << "Query OK, 1 row affected.\n";
        } else {
            std::cout << "Error: Delete failed.\n";
        }
    } else {
        // Broadcast to all workers
        for (int i = 1; i < world_size; i++)
            send_db_msg(msg, i, TAG_DELETE);

        int success_count = 0;
        for (int i = 1; i < world_size; i++) {
            int ack_status;
            MPI_Status status;
            MPI_Recv(&ack_status, 1, MPI_INT, MPI_ANY_SOURCE, TAG_ACK, MPI_COMM_WORLD, &status);
            if (ack_status == 0) success_count++;
        }

        std::cout << "Query OK, " << success_count << " row(s) affected.\n";
    }
}

void shutdown_workers(int world_size) {
    for (int i = 1; i < world_size; i++) {
        int dummy = 0;
        MPI_Send(&dummy, 1, MPI_INT, i, TAG_EXIT, MPI_COMM_WORLD);
    }
}