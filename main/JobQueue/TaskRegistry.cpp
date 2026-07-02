#include "TaskRegistry.hpp"

void TaskRegistry::set_status(const std::string& id, TaskStatus s)
{
    std::lock_guard<std::mutex> lock(status_mutex_);
    status_[id] = s;
}

TaskStatus TaskRegistry::get_status(const std::string& id) const
{
    std::lock_guard<std::mutex> lock(status_mutex_);
    return status_.at(id);
}

bool TaskRegistry::has_task(const std::string& id) const
{
    std::lock_guard<std::mutex> lock(status_mutex_);
    return status_.count(id);
}

void TaskRegistry::cancel(const std::string& id)
{
    std::lock_guard<std::mutex> lock(cancel_mutex_);
    cancelled_.insert(id);
}

bool TaskRegistry::is_cancelled(const std::string& id) const
{
    std::lock_guard<std::mutex> lock(cancel_mutex_);
    return cancelled_.count(id);
}

uint64_t TaskRegistry::register_wire_id(const std::string& task_id)
{
    uint64_t id = next_wire_id_++;

    std::lock_guard<std::mutex> lock(id_mutex_);
    wire_to_task_[id] = task_id;

    return id;
}

std::string TaskRegistry::lookup_task_id(uint64_t wire_id) const
{
    std::lock_guard<std::mutex> lock(id_mutex_);

    auto it = wire_to_task_.find(wire_id);

    if (it == wire_to_task_.end())
        return "";

    return it->second;
}
size_t TaskRegistry::done_count() const
{
    std::lock_guard<std::mutex> lock(status_mutex_);

    size_t done = 0;

    for (const auto& [id, status] : status_)
    {
        if (status == TaskStatus::Done)
            done++;
    }

    return done;
}
size_t TaskRegistry::failed_count() const
{
    std::lock_guard<std::mutex> lock(status_mutex_);

    size_t failed = 0;

    for (const auto& [id, status] : status_)
    {
        if (status == TaskStatus::Failed)
            failed++;
    }

    return failed;
}
size_t TaskRegistry::pending_count() const
{
    std::lock_guard<std::mutex> lock(status_mutex_);

    size_t pending = 0;

    for (const auto& [id, status] : status_)
    {
        if (status == TaskStatus::Pending)
            pending++;
    }

    return pending;
}