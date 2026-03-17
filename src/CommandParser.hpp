#pragma once
#include <string>

class CommandParser {
public:
    enum class CommandType {
        CREATE_DATABASE,
        DROP_DATABASE,
        USE_DATABASE,
        SHOW_DATABASES,
        SHOW_TABLES,
        CREATE_TABLE,
        DROP_TABLE,
        INSERT,
        SELECT,
        UPDATE,
        DELETE_ROW,
        EXIT,
        UNKNOWN
    };

    struct ParsedCommand {
        CommandType type;
        std::string database_name;  // For CREATE/USE/DROP DATABASE
        std::string sql;            // Original or normalized SQL
        std::string table_name;     // Extracted table name
        int shard_key;             // Extracted ID for sharding (-1 if not found)
        bool needs_all_workers;    // True for SELECT, CREATE TABLE, etc.
        bool is_valid;
        std::string error_message;

        ParsedCommand()
            : type(CommandType::UNKNOWN),
              shard_key(-1),
              needs_all_workers(false),
              is_valid(false) {}
    };

    static ParsedCommand parse(const std::string& input);

private:
    static std::string trim(const std::string& str);
    static std::string to_upper(const std::string& str);
    static int extract_shard_key(const std::string& sql);
    static std::string extract_table_name(const std::string& sql);
    static std::string extract_database_name(const std::string& input);
};
