// wal_server.cpp — minimal long-running server for testing WAL recovery.
// Place this file at the project ROOT (same level as main.cpp).
#include <chrono>
#include <iostream>
#include <thread>
#include "JobQueue/Jobqueueserver.hpp"

int main(int argc, char** argv) {
    uint16_t port = argc > 1 ? static_cast<uint16_t>(std::stoi(argv[1])) : 9600;
    std::string wal_path = argc > 2 ? argv[2] : "jobs.wal";

    std::cout << "[wal_server] starting on port " << port << " wal=" << wal_path << "\n";
    JobQueueServer server(port, 4, 4, wal_path);
    server.start();

    // Sit here until killed (SIGKILL / Stop-Process -Force simulates a crash).
    while (true) std::this_thread::sleep_for(std::chrono::seconds(1));
}