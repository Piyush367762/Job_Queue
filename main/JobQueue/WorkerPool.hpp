#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "../Data_Struct/Circular_Buffer.hpp"
#include "Task.hpp"
#include "Semaphore.hpp"
class WorkerPool {
public:
    using ExecuteFn = std::function<void(Task&)>;

    explicit WorkerPool(int n_workers, int max_concurrent, int queue_capacity, ExecuteFn execute_fn)
        : n_workers_(n_workers)
        , slots_(max_concurrent)
        , execute_fn_(std::move(execute_fn))
        , running_(false)
        , done_count_(0)
        , active_count_(0)
        , submitted_count_(0) {
        (void)queue_capacity;
        if (n_workers_ <= 0)
            throw std::invalid_argument("WorkerPool requires at least one worker");
        if (!execute_fn_)
            throw std::invalid_argument("WorkerPool requires an execute function");
    }

    ~WorkerPool() {
        shutdown();
    }

    WorkerPool(const WorkerPool&) = delete;
    WorkerPool& operator=(const WorkerPool&) = delete;

    void start() {
        bool expected = false;
        if (!running_.compare_exchange_strong(expected, true))
            return;

        threads_.reserve(static_cast<std::size_t>(n_workers_));
        for (int i = 0; i < n_workers_; ++i)
            threads_.emplace_back(&WorkerPool::worker_loop_, this, i + 1);
    }

    bool submit(Task task) {
        if (!running_) return false;
        if (!queue_.enqueue(std::move(task))) return false;
        ++submitted_count_;
        return true;
    }

    bool try_submit(Task task) {
        if (running_ && queue_.try_enqueue(std::move(task))) {
            ++submitted_count_;
            return true;
        }
        return false;
    }

    void shutdown() {
        bool expected = true;
        if (!running_.compare_exchange_strong(expected, false))
            return;

        queue_.shutdown();
        for (auto& t : threads_)
            if (t.joinable()) t.join();
    }

    struct Stats {
        int submitted;
        int done;
        int active;
        int queued;
        int workers;
        int free_slots;
    };

    Stats stats() const {
        return {
            submitted_count_.load(),
            done_count_.load(),
            active_count_.load(),
            static_cast<int>(queue_.size()),
            n_workers_,
            slots_.count()
        };
    }

    void print_stats(const std::string& label = "") const {
        auto s = stats();
        std::string prefix = label.empty() ? "" : "[" + label + "] ";
        log_("stats", prefix +
            "submitted=" + std::to_string(s.submitted) +
            "  done=" + std::to_string(s.done) +
            "  active=" + std::to_string(s.active) +
            "  queued=" + std::to_string(s.queued) +
            "  free_slots=" + std::to_string(s.free_slots));
    }

private:
    void worker_loop_(int worker_id) {
        std::string name = "worker-" + std::to_string(worker_id);
        while (true) {
            auto maybe = queue_.dequeue();
            if (!maybe.has_value()) {
                log_(name, "queue drained + shutdown -> exit");
                break;
            }

            Task task = std::move(maybe.value());
            slots_.acquire();
            ++active_count_;

            task.status = TaskStatus::Processing;
            log_(name, "start -> " + task.to_string());

            execute_fn_(task);

            task.status = TaskStatus::Done;
            int completed = ++done_count_;
            --active_count_;
            slots_.release();

            log_(name, "done  <- " + task.id +
                 "  [done=" + std::to_string(completed) + "]");
        }
    }

    void log_(const std::string& who, const std::string& msg) const {
        std::unique_lock<std::mutex> lock(log_mutex_);
        auto now = std::chrono::system_clock::now().time_since_epoch();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now).count() % 100000;
        std::cout << "[" << std::setw(5) << ms << "ms] "
                  << std::setw(12) << who << " | " << msg << "\n";
    }

    int                       n_workers_;
    Semaphore                 slots_;
    ExecuteFn                 execute_fn_;
    CircularBuffer<Task, 128> queue_;
    std::vector<std::thread>  threads_;

    std::atomic<bool> running_;
    std::atomic<int>  done_count_;
    std::atomic<int>  active_count_;
    std::atomic<int>  submitted_count_;

    mutable std::mutex log_mutex_;
};
