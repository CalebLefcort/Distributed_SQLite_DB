#pragma once
#include <cstring>
#include <string>
#include <vector>

// MPI Tags to distinguish message types
const int TAG_SQL_CMD = 1;
const int TAG_EXIT = 99;

// New tags for enhanced protocol
const int TAG_CREATE_DB = 2;
const int TAG_DROP_DB = 3;
const int TAG_USE_DB = 4;
const int TAG_TABLE_DDL = 5;
const int TAG_INSERT = 6;
const int TAG_SELECT = 7;
const int TAG_UPDATE = 8;
const int TAG_DELETE = 9;
const int TAG_RESULT = 10;
const int TAG_ACK = 11;
const int TAG_RESULT_DATA = 12;  // Variable-length data payload following a TAG_RESULT header

// Variable-length database command message.
// Wire format: [uint32 db_name_len][db_name][uint32 sql_len][sql][int32 needs_response]
struct DatabaseMessage {
    char db_name[256];
    char sql_query[4096];
    int needs_response;

    DatabaseMessage() : needs_response(0) {
        db_name[0] = '\0';
        sql_query[0] = '\0';
    }

    // Serialize to compact byte buffer (only sends actual string lengths)
    std::vector<char> serialize() const {
        uint32_t db_len = static_cast<uint32_t>(strlen(db_name));
        uint32_t sql_len = static_cast<uint32_t>(strlen(sql_query));
        size_t total = sizeof(db_len) + db_len + sizeof(sql_len) + sql_len + sizeof(needs_response);

        std::vector<char> buf(total);
        char* p = buf.data();

        memcpy(p, &db_len, sizeof(db_len));     p += sizeof(db_len);
        memcpy(p, db_name, db_len);             p += db_len;
        memcpy(p, &sql_len, sizeof(sql_len));   p += sizeof(sql_len);
        memcpy(p, sql_query, sql_len);          p += sql_len;
        memcpy(p, &needs_response, sizeof(needs_response));

        return buf;
    }

    // Deserialize from byte buffer
    void deserialize(const char* data, int size) {
        const char* p = data;
        const char* end = data + size;

        uint32_t db_len = 0;
        if (p + sizeof(db_len) > end) return;
        memcpy(&db_len, p, sizeof(db_len)); p += sizeof(db_len);
        if (db_len > 255) db_len = 255;
        if (p + db_len > end) return;
        memcpy(db_name, p, db_len); p += db_len;
        db_name[db_len] = '\0';

        uint32_t sql_len = 0;
        if (p + sizeof(sql_len) > end) return;
        memcpy(&sql_len, p, sizeof(sql_len)); p += sizeof(sql_len);
        if (sql_len > 4095) sql_len = 4095;
        if (p + sql_len > end) return;
        memcpy(sql_query, p, sql_len); p += sql_len;
        sql_query[sql_len] = '\0';

        if (p + sizeof(needs_response) <= end)
            memcpy(&needs_response, p, sizeof(needs_response));
        else
            needs_response = 0;
    }
};

// Result header from worker (sent with TAG_RESULT).
// If row_count > 0 and status == 0, a second message with the actual row data
// follows immediately on TAG_RESULT_DATA (variable length, use MPI_Probe to size it).
struct ResultMessage {
    int worker_rank;
    int row_count;
    int status;  // 0=success, 1=error
    char error_msg[512];

    ResultMessage() : worker_rank(0), row_count(0), status(0) {
        error_msg[0] = '\0';
    }
};

// Helper to determine which node gets a row (Hash Sharding)
// ID % (Workers) + 1
inline int get_target_worker(int row_id, int world_size) {
    int num_workers = world_size - 1;
    if (num_workers < 1) return 0;
    return (row_id % num_workers) + 1;
}
