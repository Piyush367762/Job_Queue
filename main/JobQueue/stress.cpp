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
#include <iostream>
#include <mutex>
#include <random>
#include <unordered_set>
#include <thread>
#include <vector>

#include "JobQueueServer.hpp"

using Clock = std::chrono::steady_clock;

static double ms_since(Clock::time_point start) {
    return std::chrono::duration<double, std::milli>(Clock::now() - start).count();
}

static bool positive(int value) { return value > 0; }

static void print_usage(const char* name) {
    std::cout << "Usage: " << name
              << " [clients] [tasks_per_client] [workers] [max_concurrent] [work_ms] [port]\n";
}

struct SubmittedTask {
    uint64_t wire_id;
    Clock::time_point submitted_at;
};

int main(int argc, char** argv) {
    if (argc > 1 && std::string(argv[1]) == "--help") {
        print_usage(argv[0]);
        return 0;
    }

    int n_clients = 50, tasks_per_client = 200, n_workers = 8;
    int max_concurrent = 16, work_ms = 5, port_value = 9191;
    try {
        if (argc > 1) n_clients = std::stoi(argv[1]);
        if (argc > 2) tasks_per_client = std::stoi(argv[2]);
        if (argc > 3) n_workers = std::stoi(argv[3]);
        if (argc > 4) max_concurrent = std::stoi(argv[4]);
        if (argc > 5) work_ms = std::stoi(argv[5]);
        if (argc > 6) port_value = std::stoi(argv[6]);
    } catch (const std::exception&) {
        std::cerr << "invalid numeric argument\n";
        print_usage(argv[0]);
        return 2;
    }
    if (!positive(n_clients) || !positive(tasks_per_client) || !positive(n_workers) ||
        !positive(max_concurrent) || work_ms < 0 || port_value < 1 || port_value > 65535) {
        std::cerr << "arguments must be positive; work_ms may be zero; port must be 1..65535\n";
        return 2;
    }
    uint16_t port = static_cast<uint16_t>(port_value);

    const long total_tasks = static_cast<long>(n_clients) * tasks_per_client;

    std::cout << "=== Stress test config ===\n"
              << "  clients:          " << n_clients << "\n"
              << "  tasks per client: " << tasks_per_client << "\n"
              << "  total tasks:      " << total_tasks << "\n"
              << "  server workers:   " << n_workers << "\n"
              << "  max_concurrent:   " << max_concurrent << "\n"
              << "  simulated work:   " << work_ms << "ms/task\n"
              << "  port:             " << port << "\n\n";

    std::remove("stress.wal");  // start clean each run
    JobQueueServer server(port, n_workers, max_concurrent, "stress.wal");
    server.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::atomic<long> submitted{0};
    std::atomic<long> submit_failures{0};
    std::mutex latency_mutex;
    std::vector<double> latencies_ms;
    latencies_ms.reserve(static_cast<std::size_t>(total_tasks));
    std::mutex task_mutex;
    std::vector<SubmittedTask> submitted_tasks;
    submitted_tasks.reserve(static_cast<std::size_t>(total_tasks));

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
                {
                    std::lock_guard<std::mutex> lock(task_mutex);
                    submitted_tasks.push_back({reply.task_id, t0});
                }
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
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
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

    // Drain: wait until every task submitted by this run reaches a terminal
    // state, then verify each returned wire ID individually.
    std::cout << "=== Draining (waiting for all tasks to finish) ===\n";
    auto drain_start = Clock::now();
    JobQueueServer::LiveStats s{};
    bool drain_timed_out = true;
    for (int i = 0; i < 6000; ++i) {  // up to 10 minutes of polling
        s = server.live_stats();
        if (static_cast<long>(s.done_count + s.failed_count) >=
            static_cast<long>(submitted_tasks.size())) {
            drain_timed_out = false;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    double drain_elapsed_ms = ms_since(drain_start);
    const auto completion_observed_at = Clock::now();
    const double end_to_end_ms =
        std::chrono::duration<double, std::milli>(completion_observed_at - submit_phase_start).count();

    socket_handle status_fd = jobq_client::connect_to("127.0.0.1", port);
    std::unordered_set<uint64_t> unknown_ids;
    std::size_t verified_done = 0;
    std::size_t verified_failed = 0;
    if (status_fd != invalid_socket_handle) {
        for (const auto& task : submitted_tasks) {
            try {
                auto reply = jobq_client::status(status_fd, task.wire_id);
                if (reply.body == "done") ++verified_done;
                else if (reply.body == "failed") ++verified_failed;
                else unknown_ids.insert(task.wire_id);
            } catch (...) {
                unknown_ids.insert(task.wire_id);
            }
        }
        close_socket(status_fd);
    } else {
        unknown_ids.reserve(submitted_tasks.size());
        for (const auto& task : submitted_tasks)
            unknown_ids.insert(task.wire_id);
    }

    std::cout << "  done:             " << s.done_count << "\n"
              << "  failed:           " << s.failed_count << "\n"
              << "  dead-lettered:    " << s.dead_letter_count << "\n"
              << "  drain time:       " << drain_elapsed_ms << " ms\n"
              << "  end-to-end time:  " << end_to_end_ms << " ms\n"
              << "  verified done:    " << verified_done << "\n"
              << "  verified failed:  " << verified_failed << "\n"
              << "  unknown IDs:      " << unknown_ids.size() << "\n"
              << "  completion tput:  " << (s.done_count / (end_to_end_ms / 1000.0))
              << " tasks/sec\n\n";
    if (drain_timed_out)
        std::cerr << "ERROR: drain timed out after 10 minutes\n";

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
    return (drain_timed_out || submit_failures.load() != 0 ||
            verified_done + verified_failed != submitted_tasks.size() ||
            !unknown_ids.empty()) ? 1 : 0;
}
