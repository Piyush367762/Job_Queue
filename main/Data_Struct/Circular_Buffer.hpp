#pragma once
#include <array>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <stdexcept>
template <typename T, std::size_t CAP>
class CircularBuffer {
public:
    CircularBuffer() : head_(0), tail_(0), size_(0) {}

    CircularBuffer(const CircularBuffer&)            = delete;
    CircularBuffer& operator=(const CircularBuffer&) = delete;


    bool enqueue(T item) {
        std::unique_lock<std::mutex> lock(mutex_);

        not_full_.wait(lock, [this] {
            return size_ < CAP || shutdown_;
        });

        if (shutdown_) return false;

        buffer_[tail_] = std::move(item);
        tail_ = (tail_ + 1) % CAP;  
        ++size_;


        not_empty_.notify_one();
        return true;
    }

    bool try_enqueue(T item) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (size_ == CAP || shutdown_) return false;

        buffer_[tail_] = std::move(item);
        tail_ = (tail_ + 1) % CAP;
        ++size_;

        not_empty_.notify_one();
        return true;
    }
    std::optional<T> dequeue() {
        std::unique_lock<std::mutex> lock(mutex_);

        not_empty_.wait(lock, [this] {
            return size_ > 0 || shutdown_;
        });

        if (size_ == 0) return std::nullopt;

        T item = std::move(buffer_[head_]);
        head_ = (head_ + 1) % CAP;   
        --size_;


        not_full_.notify_one();
        return item;
    }


    std::optional<T> peek() const {
        std::unique_lock<std::mutex> lock(mutex_);
        if (size_ == 0) return std::nullopt;
        return buffer_[head_];
    }


    void shutdown() {
        std::unique_lock<std::mutex> lock(mutex_);
        shutdown_ = true;
        not_full_.notify_all();
        not_empty_.notify_all();
    }

    std::size_t size()     const { std::unique_lock<std::mutex> l(mutex_); return size_; }
    bool        empty()    const { std::unique_lock<std::mutex> l(mutex_); return size_ == 0; }
    bool        full()     const { std::unique_lock<std::mutex> l(mutex_); return size_ == CAP; }
    std::size_t capacity() const { return CAP; }

private:

    std::array<T, CAP> buffer_;      // fixed array – lives on heap when
                                     // CircularBuffer itself is heap-allocated
    std::size_t        head_;        // read  cursor
    std::size_t        tail_;        // write cursor
    std::size_t        size_;        // live item count

    mutable std::mutex      mutex_;         // guards all state above
    std::condition_variable not_full_;      // producers wait here
    std::condition_variable not_empty_;     // consumers wait here
    bool                    shutdown_{false};
};