#pragma once
#include <string>
#include <sqlite3.h>

class Database {
private:
    sqlite3* db;
    std::string filename;
    std::string current_database;

public:
    Database(std::string name);
    ~Database();

    // For CREATE, INSERT, UPDATE, DELETE — returns true on success
    bool execute_write(const std::string& sql);

    // For SELECT (returns CSV formatted string)
    std::string execute_read(const std::string& sql);

    // Multi-database support
    bool switch_database(const std::string& db_name, int worker_rank);
    void close_current();
    std::string get_current_database() const { return current_database; }
    bool is_database_open() const { return db != nullptr; }

    // Static helper for database file naming
    static std::string get_db_filename(const std::string& db_name, int worker_rank);
};