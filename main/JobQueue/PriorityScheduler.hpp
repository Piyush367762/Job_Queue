#pragma once

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <queue>
#include <utility>
#include <vector>

#include "Task.hpp"

class PriorityScheduler {
public:
    bool push(Task task) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (shutdown_) return false;
        queue_.push(Entry{std::move(task), next_sequence_++});
        not_empty_.notify_one();
        return true;
    }

    std::optional<Task> try_pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (queue_.empty()) return std::nullopt;
        return pop_locked_();
    }

    std::optional<Task> pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock, [this] {
            return !queue_.empty() || shutdown_;
        });
        if (queue_.empty()) return std::nullopt;
        return pop_locked_();
    }

    void shutdown() {
        std::unique_lock<std::mutex> lock(mutex_);
        shutdown_ = true;
        not_empty_.notify_all();
    }

    std::size_t size() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return queue_.size();
    }

    bool empty() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return queue_.empty();
    }

private:
    struct Entry {
        Task task;
        std::uint64_t sequence;
    };

    struct Compare {
        bool operator()(const Entry& a, const Entry& b) const {
            if (a.task.priority != b.task.priority)
                return a.task.priority < b.task.priority;
            return a.sequence > b.sequence;
        }
    };

    Task pop_locked_() {
        Task task = std::move(queue_.top().task);
        queue_.pop();
        return task;
    }

    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::priority_queue<Entry, std::vector<Entry>, Compare> queue_;
    std::uint64_t next_sequence_{0};
    bool shutdown_{false};
};
