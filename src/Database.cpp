// Database.cpp - Wraps SQLite operations for a single node's database file.
// Handles opening, closing, and switching databases, and provides methods for
// executing read (SELECT) and write (INSERT/UPDATE/DELETE/DDL) SQL statements.

#include "Database.hpp"
#include <iostream>
#include <cstdio>

struct SelectCallbackState {
    std::string result;
    bool header_printed = false;
};

// Callback for SELECT queries — emits a header row on the first call, then data rows
static int select_callback(void* data, int argc, char** argv, char** azColName) {
    SelectCallbackState* state = static_cast<SelectCallbackState*>(data);
    if (!state->header_printed) {
        for (int i = 0; i < argc; i++) {
            state->result += azColName[i];
            if (i < argc - 1) state->result += ",";
        }
        state->result += "\n";
        state->header_printed = true;
    }
    for (int i = 0; i < argc; i++) {
        state->result += (argv[i] ? argv[i] : "NULL");
        if (i < argc - 1) state->result += ",";
    }
    state->result += "\n";
    return 0;
}

Database::Database(std::string name) : filename(name), db(nullptr) {
    // Open (or create) the local database file
    int rc = sqlite3_open(filename.c_str(), &db);
    if (rc) {
        std::cerr << "Can't open database: " << sqlite3_errmsg(db) << "\n";
        db = nullptr;
    }
}

Database::~Database() {
    close_current();
}

void Database::close_current() {
    if (db) {
        sqlite3_close(db);
        db = nullptr;
    }
    current_database = "";
}

bool Database::switch_database(const std::string& db_name, int worker_rank) {
    // Close current database if open
    close_current();

    // Open new database
    filename = get_db_filename(db_name, worker_rank);
    int rc = sqlite3_open(filename.c_str(), &db);
    if (rc) {
        std::cerr << "Can't open database: " << sqlite3_errmsg(db) << "\n";
        db = nullptr;
        return false;
    }

    current_database = db_name;
    return true;
}

std::string Database::get_db_filename(const std::string& db_name, int worker_rank) {
    return db_name + "_node" + std::to_string(worker_rank) + ".db";
}

bool Database::execute_write(const std::string& sql) {
    if (!db) {
        std::cerr << "Database not open\n";
        return false;
    }

    char* zErrMsg = 0;
    int rc = sqlite3_exec(db, sql.c_str(), 0, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        std::cerr << "SQL Error: " << zErrMsg << "\n";
        sqlite3_free(zErrMsg);
        return false;
    }
    return true;
}

std::string Database::execute_read(const std::string& sql) {
    if (!db) {
        return "ERROR: Database not open";
    }

    char* zErrMsg = 0;
    SelectCallbackState state;
    int rc = sqlite3_exec(db, sql.c_str(), select_callback, &state, &zErrMsg);
    if (rc != SQLITE_OK) {
        std::cerr << "SQL Error: " << zErrMsg << "\n";
        sqlite3_free(zErrMsg);
        return "ERROR";
    }
    return state.result;
}