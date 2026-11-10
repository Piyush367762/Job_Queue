#pragma once

#include <atomic>
#include <functional>
#include <stdexcept>
#include <thread>
#include <vector>

#include "../Data_Struct/Circular_Buffer.hpp"
#include "Task.hpp"
#include "Semaphore.hpp"
class WorkerPool {
public:
    using ExecuteFn = std::function<void(Task&)>;
    using CompletionFn = std::function<void(const Task&)>;

    explicit WorkerPool(int n_workers,
                        int max_concurrent,
                        ExecuteFn execute_fn,
                        CompletionFn completion_fn)
        : n_workers_(n_workers)
        , slots_(max_concurrent)
        , execute_fn_(std::move(execute_fn))
        , completion_fn_(std::move(completion_fn))
        , running_(false)
        , done_count_(0)
        , active_count_(0)
        , submitted_count_(0) {
        if (n_workers_ <= 0)
            throw std::invalid_argument("WorkerPool requires at least one worker");
        if (!execute_fn_)
            throw std::invalid_argument("WorkerPool requires an execute function");
        if (!completion_fn_)
            throw std::invalid_argument("WorkerPool requires a completion function");
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
            threads_.emplace_back(&WorkerPool::worker_loop_, this);
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

private:
    void worker_loop_() {
        while (true) {
            auto maybe = queue_.dequeue();
            if (!maybe.has_value()) {
                break;
            }

            Task task = std::move(maybe.value());
            slots_.acquire();
            ++active_count_;

            task.status = TaskStatus::Processing;

            try {
                execute_fn_(task);
            } catch (...) {
                task.status = TaskStatus::Failed;
            }

            if (task.status != TaskStatus::Failed)
                task.status = TaskStatus::Done;
            try {
                completion_fn_(task);
            } catch (...) {
                // Bookkeeping failures must not terminate a worker thread.
            }
            ++done_count_;
            --active_count_;
            slots_.release();
        }
    }

    int                       n_workers_;
    Semaphore                 slots_;
    ExecuteFn                 execute_fn_;
    CompletionFn              completion_fn_;
    CircularBuffer<Task, 128> queue_;
    std::vector<std::thread>  threads_;

    std::atomic<bool> running_;
    std::atomic<int>  done_count_;
    std::atomic<int>  active_count_;
    std::atomic<int>  submitted_count_;

};
