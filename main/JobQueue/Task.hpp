#pragma once
#include <chrono>
#include <cstdint>
#include <atomic>
#include <sstream>
#include <string>
enum class TaskStatus {
    Pending,
    Processing,
    Done,
    Failed
};
struct Task {
    std::string  id;           
    std::string  type;         
    std::string  payload;      
    int          priority{5};  
    std::int64_t created_at;   
    int          retries{0}; 
    TaskStatus   status{TaskStatus::Pending};
    static Task create(const std::string& type,const std::string& payload,int priority = 5)
    {
        Task t;
        t.id         = generate_id();
        t.type       = type;
        t.payload    = payload;
        t.priority   = priority;
        t.created_at = now_ms();
        return t;
    }
    std::string status_str() const {
        switch (status) {
            case TaskStatus::Pending:    return "pending";
            case TaskStatus::Processing: return "processing";
            case TaskStatus::Done:       return "done";
            case TaskStatus::Failed:     return "failed";
        }
        return "unknown";
    }
    std::string to_string() const {
        return "[" + id + "] type=" + type +
               " priority=" + std::to_string(priority) +
               " status=" + status_str() +
               " payload=" + payload;
    }
 
private:
    static std::string generate_id() {
        static std::atomic<std::uint64_t> sequence{1};
        std::ostringstream oss;
        oss << std::hex << now_ms() << '-' << sequence.fetch_add(1, std::memory_order_relaxed);
        return oss.str();
    }
 
    static std::int64_t now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }
};
