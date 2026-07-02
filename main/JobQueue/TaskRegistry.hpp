#pragma once

#include <atomic>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "Task.hpp"

class TaskRegistry
{
public:

    // ---------- Status ----------

    void set_status(const std::string&, TaskStatus);

    TaskStatus get_status(const std::string&) const;

    bool has_task(const std::string&) const;


    // ---------- Cancellation ----------

    void cancel(const std::string&);

    bool is_cancelled(const std::string&) const;


    // ---------- Wire IDs ----------

    uint64_t register_wire_id(const std::string&);

    std::string lookup_task_id(uint64_t) const;


    // ---------- Statistics ----------

    size_t done_count() const;

    size_t failed_count() const;

    size_t pending_count() const;

private:

mutable std::mutex status_mutex_;
std::unordered_map<std::string, TaskStatus> status_;

mutable std::mutex cancel_mutex_;
std::unordered_set<std::string> cancelled_;

mutable std::mutex id_mutex_;
std::unordered_map<uint64_t, std::string> wire_to_task_;

std::atomic<uint64_t> next_wire_id_{1};
};