// Worker.cpp - Runs on each non-zero MPI rank as a worker node. Listens for
// messages from the coordinator and executes database operations (create, drop,
// use, DDL, insert, update, delete, select) on its local SQLite shard, sending
// results or acknowledgements back to the coordinator.

#include "Worker.hpp"
#include "Database.hpp"
#include "Protocol.hpp"
#include <mpi.h>
#include <iostream>
#include <cstring>
#include <cstdio>
#include <vector>

// Helper: receive a variable-length DatabaseMessage using MPI_Probe + deserialize
static DatabaseMessage recv_db_msg(int source, int tag, MPI_Status& status) {
    int msg_size;
    MPI_Get_count(&status, MPI_BYTE, &msg_size);

    std::vector<char> buf(msg_size);
    MPI_Recv(buf.data(), msg_size, MPI_BYTE, source, tag, MPI_COMM_WORLD, &status);

    DatabaseMessage msg;
    msg.deserialize(buf.data(), msg_size);
    return msg;
}

void run_worker(int rank) {
    std::string current_db_name = "";
    Database* local_db = nullptr;

    std::cout << "[Worker " << rank << "] Online. Ready for commands.\n" << std::flush;

    while (true) {
        MPI_Status status;

        // Block until a message arrives
        MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        switch (status.MPI_TAG) {
            case TAG_EXIT: {
                int dummy;
                MPI_Recv(&dummy, 1, MPI_INT, 0, TAG_EXIT, MPI_COMM_WORLD, &status);
                std::cout << "[Worker " << rank << "] Shutting down.\n" << std::flush;
                if (local_db) delete local_db;
                return;
            }

            case TAG_CREATE_DB: {
                DatabaseMessage msg = recv_db_msg(0, TAG_CREATE_DB, status);

                // Create new database file
                std::string db_filename = Database::get_db_filename(msg.db_name, rank);
                Database temp_db(db_filename);

                int ack_status = temp_db.is_database_open() ? 0 : 1;
                MPI_Send(&ack_status, 1, MPI_INT, 0, TAG_ACK, MPI_COMM_WORLD);

                std::cout << "[Worker " << rank << "] Created database: " << msg.db_name << "\n" << std::flush;
                break;
            }

            case TAG_DROP_DB: {
                DatabaseMessage msg = recv_db_msg(0, TAG_DROP_DB, status);

                // Close database if it's currently open
                if (local_db && current_db_name == msg.db_name) {
                    delete local_db;
                    local_db = nullptr;
                    current_db_name = "";
                }

                // Remove database file
                std::string db_filename = Database::get_db_filename(msg.db_name, rank);
                int result = std::remove(db_filename.c_str());

                int ack_status = (result == 0) ? 0 : 1;
                MPI_Send(&ack_status, 1, MPI_INT, 0, TAG_ACK, MPI_COMM_WORLD);

                std::cout << "[Worker " << rank << "] Dropped database: " << msg.db_name << "\n" << std::flush;
                break;
            }

            case TAG_USE_DB: {
                DatabaseMessage msg = recv_db_msg(0, TAG_USE_DB, status);

                // Switch to new database
                if (local_db) {
                    delete local_db;
                }

                std::string db_filename = Database::get_db_filename(msg.db_name, rank);
                local_db = new Database(db_filename);
                current_db_name = msg.db_name;

                int ack_status = local_db->is_database_open() ? 0 : 1;
                MPI_Send(&ack_status, 1, MPI_INT, 0, TAG_ACK, MPI_COMM_WORLD);

                std::cout << "[Worker " << rank << "] Switched to database: " << msg.db_name << "\n" << std::flush;
                break;
            }

            case TAG_TABLE_DDL: {
                DatabaseMessage msg = recv_db_msg(0, TAG_TABLE_DDL, status);

                int ack_status = 1;  // Error by default
                if (local_db && local_db->is_database_open()) {
                    ack_status = local_db->execute_write(msg.sql_query) ? 0 : 1;
                    if (ack_status == 0)
                        std::cout << "[Worker " << rank << "] Executed DDL.\n" << std::flush;
                } else {
                    std::cerr << "[Worker " << rank << "] Error: No database selected.\n";
                }

                MPI_Send(&ack_status, 1, MPI_INT, 0, TAG_ACK, MPI_COMM_WORLD);
                break;
            }

            case TAG_INSERT: {
                DatabaseMessage msg = recv_db_msg(0, TAG_INSERT, status);

                int ack_status = 1;  // Error by default
                if (local_db && local_db->is_database_open()) {
                    ack_status = local_db->execute_write(msg.sql_query) ? 0 : 1;
                    if (ack_status == 0)
                        std::cout << "[Worker " << rank << "] Executed INSERT.\n" << std::flush;
                } else {
                    std::cerr << "[Worker " << rank << "] Error: No database selected.\n";
                }

                MPI_Send(&ack_status, 1, MPI_INT, 0, TAG_ACK, MPI_COMM_WORLD);
                break;
            }

            case TAG_UPDATE: {
                DatabaseMessage msg = recv_db_msg(0, TAG_UPDATE, status);

                int ack_status = 1;  // Error by default
                if (local_db && local_db->is_database_open()) {
                    ack_status = local_db->execute_write(msg.sql_query) ? 0 : 1;
                    if (ack_status == 0)
                        std::cout << "[Worker " << rank << "] Executed UPDATE.\n" << std::flush;
                } else {
                    std::cerr << "[Worker " << rank << "] Error: No database selected.\n";
                }

                MPI_Send(&ack_status, 1, MPI_INT, 0, TAG_ACK, MPI_COMM_WORLD);
                break;
            }

            case TAG_DELETE: {
                DatabaseMessage msg = recv_db_msg(0, TAG_DELETE, status);

                int ack_status = 1;  // Error by default
                if (local_db && local_db->is_database_open()) {
                    ack_status = local_db->execute_write(msg.sql_query) ? 0 : 1;
                    if (ack_status == 0)
                        std::cout << "[Worker " << rank << "] Executed DELETE.\n" << std::flush;
                } else {
                    std::cerr << "[Worker " << rank << "] Error: No database selected.\n";
                }

                MPI_Send(&ack_status, 1, MPI_INT, 0, TAG_ACK, MPI_COMM_WORLD);
                break;
            }

            case TAG_SELECT: {
                DatabaseMessage msg = recv_db_msg(0, TAG_SELECT, status);

                ResultMessage res_msg;
                res_msg.worker_rank = rank;
                std::string result_data;

                if (local_db && local_db->is_database_open()) {
                    std::string result = local_db->execute_read(msg.sql_query);

                    if (result != "ERROR" && !result.empty()) {
                        res_msg.row_count = 0;
                        for (char c : result) {
                            if (c == '\n') res_msg.row_count++;
                        }
                        // Subtract 1 for the header line (always emitted by select_callback)
                        if (res_msg.row_count > 0) res_msg.row_count--;
                        res_msg.status = 0;
                        result_data = std::move(result);
                        std::cout << "[Worker " << rank << "] Found " << res_msg.row_count << " row(s).\n" << std::flush;
                    } else {
                        res_msg.status = 0;
                        res_msg.row_count = 0;
                    }
                } else {
                    res_msg.status = 1;
                    res_msg.row_count = 0;
                    strncpy(res_msg.error_msg, "No database selected", sizeof(res_msg.error_msg) - 1);
                    res_msg.error_msg[sizeof(res_msg.error_msg) - 1] = '\0';
                }

                // Phase 1: send fixed-size header
                MPI_Send(&res_msg, sizeof(ResultMessage), MPI_BYTE, 0, TAG_RESULT, MPI_COMM_WORLD);

                // Phase 2: send variable-length data if there are rows
                if (res_msg.status == 0 && res_msg.row_count > 0) {
                    MPI_Send(result_data.data(), static_cast<int>(result_data.size()),
                             MPI_CHAR, 0, TAG_RESULT_DATA, MPI_COMM_WORLD);
                }
                break;
            }

            default:
                std::cerr << "[Worker " << rank << "] Unknown message tag: " << status.MPI_TAG << "\n";
                break;
        }
    }
}
