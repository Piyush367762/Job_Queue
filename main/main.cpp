// main.cpp — functional end-to-end demo of the whole stack.
// See JobQueueServer.hpp for the data-flow diagram and the actual glue code.

#include <chrono>
#include <iostream>
#include <thread>

#include "JobQueue/Jobqueueserver.hpp"

int main(int argc, char** argv) {
    uint16_t port = 9090;
    if (argc > 1) port = static_cast<uint16_t>(std::stoi(argv[1]));

    JobQueueServer server(port, /*n_workers=*/4, /*max_concurrent=*/2, "log.file",true);
    server.start();
    std::cout << "[server] listening on port " << server.port() << "\n";

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    try {
        socket_handle fd = jobq_client::connect_to("127.0.0.1", port);
        if (fd == invalid_socket_handle) throw std::runtime_error("could not connect");

        std::cout << "\n[demo] submitting tasks with mixed priorities...\n";

        auto low   = jobq_client::submit(fd, 1, "report", "sleep:200");
        auto high1 = jobq_client::submit(fd, 9, "alert",  "sleep:50");
        auto high2 = jobq_client::submit(fd, 9, "alert",  "sleep:50");
        auto mid   = jobq_client::submit(fd, 5, "resize", "sleep:80");

        std::cout << "[demo] queued: low.id=" << low.body
                  << " high1.id=" << high1.body
                  << " high2.id=" << high2.body
                  << " mid.id="   << mid.body << "\n";

        auto wire_ids = {low.task_id, high1.task_id, high2.task_id, mid.task_id};
        bool all_done = false;
        for (int attempt = 0; attempt < 50 && !all_done; ++attempt) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            all_done = true;
            for (auto id : wire_ids) {
                auto s = jobq_client::status(fd, id);
                if (s.body != "done" && s.body != "failed") all_done = false;
            }
        }

        std::cout << "[demo] final statuses:\n";
        for (auto id : wire_ids) {
            auto s = jobq_client::status(fd, id);
            std::cout << "  wire_id=" << id << " -> " << s.body << "\n";
        }

        auto cancel_me = jobq_client::submit(fd, 3, "cleanup", "sleep:500");
        proto::Frame cancel_req;
        cancel_req.cmd = proto::Cmd::CANCEL;
        cancel_req.task_id = cancel_me.task_id;
        auto cancel_reply = jobq_client::roundtrip(fd, cancel_req);
        std::cout << "[demo] cancel reply: " << cancel_reply.body << "\n";

        close_socket(fd);
    } catch (const std::exception& e) {
        std::cerr << "[demo] error: " << e.what() << "\n";
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    std::cout << "\n[server] shutting down\n";
    server.stop();
    return 0;
}