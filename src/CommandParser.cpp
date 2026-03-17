#include "CommandParser.hpp"
#include <algorithm>
#include <cctype>
#include <regex>

std::string CommandParser::trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t\n\r");
    if (first == std::string::npos) return "";
    size_t last = str.find_last_not_of(" \t\n\r");
    return str.substr(first, (last - first + 1));
}

std::string CommandParser::to_upper(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::toupper);
    return result;
}

std::string CommandParser::extract_database_name(const std::string& input) {
    std::regex db_regex(R"(\s+(\w+))", std::regex::icase);
    std::smatch match;
    if (std::regex_search(input, match, db_regex) && match.size() > 1) {
        return match[1].str();
    }
    return "";
}

int CommandParser::extract_shard_key(const std::string& sql) {
    // Try to extract ID from INSERT INTO table VALUES (id, ...)
    std::regex insert_regex(R"(VALUES\s*\(\s*(\d+))", std::regex::icase);
    std::smatch match;
    if (std::regex_search(sql, match, insert_regex) && match.size() > 1) {
        return std::stoi(match[1].str());
    }

    // Try to extract ID from WHERE id = value or WHERE id=value
    std::regex where_regex(R"(WHERE\s+id\s*=\s*(\d+))", std::regex::icase);
    if (std::regex_search(sql, match, where_regex) && match.size() > 1) {
        return std::stoi(match[1].str());
    }

    return -1;  // Not found
}

std::string CommandParser::extract_table_name(const std::string& sql) {
    // Try to extract table name from various SQL patterns
    std::regex table_regex(R"((?:FROM|INTO|TABLE|UPDATE|JOIN)\s+(\w+))", std::regex::icase);
    std::smatch match;
    if (std::regex_search(sql, match, table_regex) && match.size() > 1) {
        return match[1].str();
    }
    return "";
}

CommandParser::ParsedCommand CommandParser::parse(const std::string& input) {
    ParsedCommand cmd;
    std::string trimmed = trim(input);

    if (trimmed.empty()) {
        cmd.error_message = "Empty command";
        return cmd;
    }

    // Remove trailing semicolon if present
    if (trimmed.back() == ';') {
        trimmed = trimmed.substr(0, trimmed.length() - 1);
        trimmed = trim(trimmed);
    }

    std::string upper = to_upper(trimmed);

    if (upper == "EXIT" || upper == "QUIT") {
        cmd.type = CommandType::EXIT;
        cmd.is_valid = true;
        return cmd;
    }

    if (upper == "SHOW DATABASES") {
        cmd.type = CommandType::SHOW_DATABASES;
        cmd.is_valid = true;
        return cmd;
    }

    if (upper == "SHOW TABLES") {
        cmd.type = CommandType::SHOW_TABLES;
        cmd.is_valid = true;
        return cmd;
    }

    if (upper.find("CREATE DATABASE") == 0) {
        cmd.type = CommandType::CREATE_DATABASE;
        cmd.database_name = extract_database_name(trimmed.substr(15));
        cmd.sql = trimmed;
        if (cmd.database_name.empty()) {
            cmd.error_message = "Invalid CREATE DATABASE syntax. Use: CREATE DATABASE <name>";
            return cmd;
        }
        cmd.is_valid = true;
        return cmd;
    }

    if (upper.find("DROP DATABASE") == 0) {
        cmd.type = CommandType::DROP_DATABASE;
        cmd.database_name = extract_database_name(trimmed.substr(13));
        cmd.sql = trimmed;
        if (cmd.database_name.empty()) {
            cmd.error_message = "Invalid DROP DATABASE syntax. Use: DROP DATABASE <name>";
            return cmd;
        }
        cmd.is_valid = true;
        return cmd;
    }

    if (upper.find("USE") == 0) {
        cmd.type = CommandType::USE_DATABASE;
        cmd.database_name = extract_database_name(trimmed.substr(3));
        cmd.sql = trimmed;
        if (cmd.database_name.empty()) {
            cmd.error_message = "Invalid USE syntax. Use: USE <database>";
            return cmd;
        }
        cmd.is_valid = true;
        return cmd;
    }

    if (upper.find("CREATE TABLE") == 0) {
        cmd.type = CommandType::CREATE_TABLE;
        cmd.sql = trimmed;
        cmd.table_name = extract_table_name(trimmed);
        cmd.needs_all_workers = true;
        cmd.is_valid = true;
        return cmd;
    }

    if (upper.find("DROP TABLE") == 0) {
        cmd.type = CommandType::DROP_TABLE;
        cmd.sql = trimmed;
        cmd.table_name = extract_table_name(trimmed);
        cmd.needs_all_workers = true;
        cmd.is_valid = true;
        return cmd;
    }

    if (upper.find("INSERT") == 0) {
        cmd.type = CommandType::INSERT;
        cmd.sql = trimmed;
        cmd.table_name = extract_table_name(trimmed);
        cmd.shard_key = extract_shard_key(trimmed);
        cmd.needs_all_workers = (cmd.shard_key == -1);  // If no ID, broadcast
        cmd.is_valid = true;
        return cmd;
    }

    if (upper.find("SELECT") == 0) {
        cmd.type = CommandType::SELECT;
        cmd.sql = trimmed;
        cmd.table_name = extract_table_name(trimmed);
        cmd.shard_key = extract_shard_key(trimmed);  // Check for WHERE id = X
        cmd.needs_all_workers = (cmd.shard_key == -1);  // Query all if no specific ID
        cmd.is_valid = true;
        return cmd;
    }

    if (upper.find("UPDATE") == 0) {
        cmd.type = CommandType::UPDATE;
        cmd.sql = trimmed;
        cmd.table_name = extract_table_name(trimmed);
        cmd.shard_key = extract_shard_key(trimmed);
        cmd.needs_all_workers = (cmd.shard_key == -1);  // If no ID, broadcast
        cmd.is_valid = true;
        return cmd;
    }

    if (upper.find("DELETE") == 0) {
        cmd.type = CommandType::DELETE_ROW;
        cmd.sql = trimmed;
        cmd.table_name = extract_table_name(trimmed);
        cmd.shard_key = extract_shard_key(trimmed);
        cmd.needs_all_workers = (cmd.shard_key == -1);  // If no ID, broadcast
        cmd.is_valid = true;
        return cmd;
    }

    cmd.error_message = "Unknown command. Type 'exit' to quit.";
    return cmd;
}
