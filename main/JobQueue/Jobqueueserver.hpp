#pragma once
#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "../Networking/Protocol.hpp"
#include "../Networking/TCPServer.hpp"

#include "../Data_Struct/Circular_Buffer.hpp"
#include "PriorityScheduler.hpp"
#include "CommandProcessor.hpp"
#include "TaskRegistry.hpp"
#include "Task.hpp"
#include "WAL.hpp"
#include "WorkerPool.hpp"
#include "job_client.hpp"
class JobQueueServer {
public:
    struct LiveStats {
        std::size_t scheduler_queued;  // waiting in PriorityScheduler
        int pool_queued;               // waiting in WorkerPool's Circular_Buffer
        int pool_active;               // currently executing
        int pool_free_slots;           // concurrency headroom left
        std::size_t done_count;        // tasks whose final status is Done
        std::size_t failed_count;
        std::size_t submitted_count;
    };
JobQueueServer(uint16_t port,
               int n_workers,
               int max_concurrent,
               std::string wal_path,
               bool verbose_worker_logs)
    : wal_(std::move(wal_path)),
      scheduler_(),
      pool_(n_workers,
            max_concurrent,
            4096,
            [this](Task& t) { execute_(t); }),
          tcp_(port,
           [this](const proto::Frame& f)
           {
               return processor_.handle_frame_(f);
           }),
      registry_(),
      processor_(wal_, scheduler_, registry_),
      verbose_(verbose_worker_logs)
{
    recover_from_wal_();
}
    ~JobQueueServer() { stop(); }

    void start() {
        pool_.start();
        dispatcher_running_ = true;
        dispatcher_ = std::thread(&JobQueueServer::dispatch_loop_, this);
        tcp_.start();
    }

    void stop() {
        bool expected = true;
        if (!dispatcher_running_.compare_exchange_strong(expected, false))
            return;

        tcp_.stop();
        scheduler_.shutdown();
        if (dispatcher_.joinable()) dispatcher_.join();
        pool_.shutdown();
    }

    uint16_t port() const { return tcp_.port(); }

    // Introspection used by the stress harness / dashboards. Cheap: just
    // takes a couple of locks + calls into WorkerPool::stats().
    LiveStats live_stats() {
        auto ps = pool_.stats();

        return LiveStats{
            scheduler_.size(),
            ps.queued,
            ps.active,
            ps.free_slots,
            registry_.done_count(),
            registry_.failed_count(),
            static_cast<std::size_t>(ps.submitted)
        };
    }

private:
    void recover_from_wal_() {
        auto records = wal_.replay();
        std::unordered_map<std::string, Task> latest;
        for (auto& t : records) latest[t.id] = std::move(t);

        int resumed = 0, restored_done = 0;
        for (auto& [id, task] : latest) {
            registry_.set_status(id, task.status);

            if (task.status == TaskStatus::Pending || task.status == TaskStatus::Processing) {
                task.status = TaskStatus::Pending;
                registry_.set_status(id, TaskStatus::Pending);
                scheduler_.push(task);
                ++resumed;
            } else {
                ++restored_done;
            }
        }
        if (resumed || restored_done) {
            std::cout << "[server] WAL recovery: " << resumed
                      << " task(s) requeued, " << restored_done
                      << " already finished\n";
        }
    }

    void dispatch_loop_() {
        while (true) {
            auto maybe_task = scheduler_.pop();
            if (!maybe_task.has_value()) break;

            Task task = std::move(maybe_task.value());

            if (registry_.is_cancelled(task.id)) {
                registry_.set_status(task.id, TaskStatus::Failed);
                wal_.append(task);
                continue;
            }

            registry_.set_status(task.id, TaskStatus::Processing);
            if (!pool_.submit(task)) {
                registry_.set_status(task.id, TaskStatus::Failed);
            }
        }
    }

    void execute_(Task& task) {
        int work_ms = 20;
        auto pos = task.payload.find("sleep:");
        if (pos != std::string::npos) {
            try { work_ms = std::stoi(task.payload.substr(pos + 6)); } catch (...) {}
        }
        if (work_ms > 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(work_ms));

        task.status = TaskStatus::Done;
        registry_.set_status(task.id, TaskStatus::Done);
        wal_.append(task);
        (void)verbose_;
    }
    private:
    WAL               wal_; //write-ahead log (write on disc)
    PriorityScheduler scheduler_;//stores job waiting to run
    WorkerPool        pool_;//owns all worker thread
    TCPServer         tcp_;//own networking
    TaskRegistry registry_;
    CommandProcessor processor_;
    bool              verbose_;

    std::atomic<bool> dispatcher_running_{false};
    std::thread       dispatcher_;//control dispatch loop
    //move from PriorityScheduler to WorkerPool


};
