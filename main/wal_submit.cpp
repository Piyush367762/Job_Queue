// wal_submit.cpp — submits N slow tasks to a running server and exits
// immediately WITHOUT waiting for completion, so you can kill the server
// mid-flight.
// Place this file at the project ROOT (same level as main.cpp).
#include <iostream>
#include <string>
#include "JobQueue/Jobqueueserver.hpp"


int main(int argc, char** argv) {
    uint16_t port = argc > 1 ? static_cast<uint16_t>(std::stoi(argv[1])) : 9600;
    int n = argc > 2 ? std::stoi(argv[2]) : 5;
    int sleep_ms = argc > 3 ? std::stoi(argv[3]) : 5000;

    socket_handle fd = jobq_client::connect_to("127.0.0.1", port);
    if (fd == invalid_socket_handle) {
        std::cerr << "could not connect\n";
        return 1;
    }
    for (int i = 0; i < n; ++i) {
        auto reply = jobq_client::submit(fd, 5, "slow-job", "sleep:" + std::to_string(sleep_ms));
        std::cout << "submitted wire_id=" << reply.task_id << " internal_id=" << reply.body << "\n";
    }
    close_socket(fd);
    return 0;
}