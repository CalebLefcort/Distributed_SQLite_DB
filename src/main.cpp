// main.cpp - Entry point for the distributed SQLite system. Initializes MPI,
// determines whether this process is the coordinator (rank 0) or a worker node,
// and dispatches to the appropriate role.

#include <mpi.h>
#include <string>
#include "Coordinator.hpp"
#include "Worker.hpp"

int main(int argc, char** argv) {
    // 1. Start MPI
    MPI_Init(&argc, &argv);

    int world_rank, world_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    // Parse --file <path> (coordinator only; workers ignore it)
    std::string sql_file;
    for (int i = 1; i + 1 < argc; i++) {
        if (std::string(argv[i]) == "--file") {
            sql_file = argv[i + 1];
            break;
        }
    }

    // 2. Branch Logic
    if (world_rank == 0) {
        run_coordinator(world_size, sql_file);
    } else {
        run_worker(world_rank);
    }

    // 3. Clean up
    MPI_Finalize();
    return 0;
}