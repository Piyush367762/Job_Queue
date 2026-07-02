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

class Semaphore {
public:
    explicit Semaphore(int initial_count)
        : count_(initial_count) {
        if (initial_count <= 0)
            throw std::invalid_argument("Semaphore initial count must be positive");
    }

    void acquire() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return count_ > 0; });
        --count_;
    }

    bool try_acquire() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (count_ == 0) return false;
        --count_;
        return true;
    }

    void release() {
        std::unique_lock<std::mutex> lock(mutex_);
        ++count_;
        cv_.notify_one();
    }

    int count() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return count_;
    }

private:
    int                     count_;
    mutable std::mutex      mutex_;
    std::condition_variable cv_;
};
