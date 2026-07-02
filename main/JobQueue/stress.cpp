// stress_test.cpp
//
// Load-tests the whole stack (TCPServer -> Protocol -> WAL ->
// PriorityScheduler -> WorkerPool -> Circular_Buffer) exactly the way a
// real deployment would be exercised: many concurrent TCP client
// connections hitting one JobQueueServer instance.
//
// Usage:
//   ./stress_test [clients] [tasks_per_client] [workers] [max_concurrent] [work_ms] [port]
//
// Example:
//   ./stress_test 50 200 8 16 5 9191
//     -> 50 concurrent connections, 200 SUBMITs each (10,000 tasks total),
//        server has 8 worker threads / 16 concurrent slots, each task
//        simulates 5ms of "work".
//
// What it measures:
//   1. SUBMIT round-trip latency (min/avg/p50/p95/p99/max) — this covers
//      TCPServer accept+recv, Protocol decode/encode, WAL::append,
//      PriorityScheduler::push, and the ACK reply.
//   2. Submission throughput (submits/sec) across all client connections.
//   3. Completion throughput (tasks/sec) — how fast WorkerPool actually
//      drains the PriorityScheduler once max_concurrent caps in.
//   4. A live queue-depth sample every 200ms while the run is in flight,
//      so you can see backpressure building on Circular_Buffer /
//      PriorityScheduler if submission outruns processing.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include "JobQueueServer.hpp"

using Clock = std::chrono::steady_clock;

static double ms_since(Clock::time_point start) {
    return std::chrono::duration<double, std::milli>(Clock::now() - start).count();
}

int main(int argc, char** argv) {
    int n_clients        = argc > 1 ? std::atoi(argv[1]) : 50;
    int tasks_per_client  = argc > 2 ? std::atoi(argv[2]) : 200;
    int n_workers         = argc > 3 ? std::atoi(argv[3]) : 8;
    int max_concurrent    = argc > 4 ? std::atoi(argv[4]) : 16;
    int work_ms           = argc > 5 ? std::atoi(argv[5]) : 5;
    uint16_t port         = argc > 6 ? static_cast<uint16_t>(std::atoi(argv[6])) : 9191;

    long total_tasks = static_cast<long>(n_clients) * tasks_per_client;

    std::cout << "=== Stress test config ===\n"
              << "  clients:          " << n_clients << "\n"
              << "  tasks per client: " << tasks_per_client << "\n"
              << "  total tasks:      " << total_tasks << "\n"
              << "  server workers:   " << n_workers << "\n"
              << "  max_concurrent:   " << max_concurrent << "\n"
              << "  simulated work:   " << work_ms << "ms/task\n"
              << "  port:             " << port << "\n\n";

    std::remove("stress.wal");  // start clean each run
    JobQueueServer server(port, n_workers, max_concurrent, "stress.wal", /*verbose=*/false);
    server.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::atomic<long> submitted{0};
    std::atomic<long> submit_failures{0};
    std::mutex latency_mutex;
    std::vector<double> latencies_ms;
    latencies_ms.reserve(static_cast<std::size_t>(total_tasks));

    auto client_work = [&](int client_idx) {
        socket_handle fd = jobq_client::connect_to("127.0.0.1", port);
        if (fd == invalid_socket_handle) {
            submit_failures += tasks_per_client;
            return;
        }

        std::mt19937 rng(static_cast<unsigned>(client_idx) * 7919u + 13);
        std::uniform_int_distribution<int> prio_dist(1, 9);
        std::vector<double> local_latencies;
        local_latencies.reserve(static_cast<std::size_t>(tasks_per_client));

        for (int i = 0; i < tasks_per_client; ++i) {
            int priority = prio_dist(rng);
            std::string payload = "sleep:" + std::to_string(work_ms);
            auto t0 = Clock::now();
            try {
                auto reply = jobq_client::submit(fd, priority, "stress", payload);
                (void)reply;
                local_latencies.push_back(ms_since(t0));
                ++submitted;
            } catch (...) {
                ++submit_failures;
            }
        }

        close_socket(fd);
        std::lock_guard<std::mutex> lock(latency_mutex);
        latencies_ms.insert(latencies_ms.end(), local_latencies.begin(), local_latencies.end());
    };

    std::cout << "=== Submitting ===\n";
    auto submit_phase_start = Clock::now();

    std::vector<std::thread> clients;
    clients.reserve(static_cast<std::size_t>(n_clients));
    for (int i = 0; i < n_clients; ++i)
        clients.emplace_back(client_work, i);

    // Live dashboard while submission + draining is happening.
    std::atomic<bool> submitting{true};
    std::thread monitor([&] {
        while (submitting.load()) {
            auto s = server.live_stats();
            std::cout << "  [t=" << static_cast<long>(ms_since(submit_phase_start)) << "ms] "
                      << "scheduler_queued=" << s.scheduler_queued
                      << " pool_queued=" << s.pool_queued
                      << " active=" << s.pool_active
                      << " free_slots=" << s.pool_free_slots
                      << " done=" << s.done_count
                      << " submitted=" << submitted.load() << "\n";
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    });

    for (auto& t : clients) t.join();
    double submit_elapsed_ms = ms_since(submit_phase_start);
    submitting = false;
    monitor.join();

    std::cout << "\n=== Submission phase complete ===\n"
              << "  submitted:        " << submitted.load() << "\n"
              << "  submit failures:  " << submit_failures.load() << "\n"
              << "  elapsed:          " << submit_elapsed_ms << " ms\n"
              << "  submit throughput:" << (submitted.load() / (submit_elapsed_ms / 1000.0))
              << " submits/sec\n\n";

    // Drain: wait for WorkerPool to finish everything still in flight.
    std::cout << "=== Draining (waiting for all tasks to finish) ===\n";
    auto drain_start = Clock::now();
    JobQueueServer::LiveStats s{};
    for (int i = 0; i < 6000; ++i) {  // up to 10 minutes of polling, exits early normally
        s = server.live_stats();
        if (static_cast<long>(s.done_count + s.failed_count) >= submitted.load()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    double drain_elapsed_ms = ms_since(drain_start);
    double total_elapsed_ms = submit_elapsed_ms + drain_elapsed_ms;

    std::cout << "  done:             " << s.done_count << "\n"
              << "  failed:           " << s.failed_count << "\n"
              << "  drain time:       " << drain_elapsed_ms << " ms\n"
              << "  end-to-end time:  " << total_elapsed_ms << " ms\n"
              << "  completion tput:  " << (s.done_count / (total_elapsed_ms / 1000.0))
              << " tasks/sec\n\n";

    // Latency percentiles for the SUBMIT round trip itself.
    std::sort(latencies_ms.begin(), latencies_ms.end());
    auto pct = [&](double p) -> double {
        if (latencies_ms.empty()) return 0.0;
        std::size_t idx = static_cast<std::size_t>(p * (latencies_ms.size() - 1));
        return latencies_ms[idx];
    };
    double sum = 0;
    for (double v : latencies_ms) sum += v;
    double avg = latencies_ms.empty() ? 0.0 : sum / latencies_ms.size();

    std::cout << "=== SUBMIT latency (ms) ===\n"
              << "  min: " << (latencies_ms.empty() ? 0.0 : latencies_ms.front()) << "\n"
              << "  p50: " << pct(0.50) << "\n"
              << "  p95: " << pct(0.95) << "\n"
              << "  p99: " << pct(0.99) << "\n"
              << "  max: " << (latencies_ms.empty() ? 0.0 : latencies_ms.back()) << "\n"
              << "  avg: " << avg << "\n";

    server.stop();
    return 0;
}