  #pragma once
  #include "../Networking/Protocol.hpp" 
  #include "Task.hpp"
  #include "WAL.hpp"
  #include "PriorityScheduler.hpp"
  #include "TaskRegistry.hpp"

    class CommandProcessor{
        public:
        CommandProcessor(
        WAL& wal,
        PriorityScheduler& scheduler,
        TaskRegistry& registry)
        : wal_(wal),
        scheduler_(scheduler),
        registry_(registry)
        {}
    proto::Frame handle_frame_(const proto::Frame& f) {
        switch (f.cmd) {
            case proto::Cmd::SUBMIT: return handle_submit_(f);
            case proto::Cmd::STATUS: return handle_status_(f);
            case proto::Cmd::CANCEL: return handle_cancel_(f);
            default: {
                proto::Frame err;
                err.cmd = proto::Cmd::ERROR;
                err.body = "unsupported command";
                return err;
            }
        }
    }

    // SUBMIT body: "priority|type|payload"
    proto::Frame handle_submit_(const proto::Frame& f) {
        int priority = 5;
        std::string type = "generic";
        std::string payload = f.body;

        auto p1 = f.body.find('|');
        if (p1 != std::string::npos) {
            try { priority = std::stoi(f.body.substr(0, p1)); } catch (...) {}
            auto rest = f.body.substr(p1 + 1);
            auto p2 = rest.find('|');
            if (p2 != std::string::npos) {
                type = rest.substr(0, p2);
                payload = rest.substr(p2 + 1);
            } else {
                type = rest;
                payload = "";
            }
        }

        Task task = Task::create(type, payload, priority);
        wal_.append(task);
        registry_.set_status(task.id, TaskStatus::Pending);
        scheduler_.push(task);

        uint64_t wire_id = registry_.register_wire_id(task.id);

        proto::Frame reply;
        reply.cmd = proto::Cmd::ACK;
        reply.task_id = wire_id;
        reply.body = task.id;
        return reply;
    }

    proto::Frame handle_status_(const proto::Frame& f) {
        proto::Frame reply;
        reply.cmd = proto::Cmd::STATUS_R;
        reply.task_id = f.task_id;

        std::string task_id = registry_.lookup_task_id(f.task_id);
        if (task_id.empty()) {
            reply.body = "unknown";
            return reply;
        }

        if (!registry_.has_task(task_id))
        {
            reply.body = "unknown";
        }
        else
        {
            reply.body = status_to_str_(registry_.get_status(task_id));
        }
        return reply;
    }

    proto::Frame handle_cancel_(const proto::Frame& f) {
        proto::Frame reply;
        reply.cmd = proto::Cmd::ACK;
        reply.task_id = f.task_id;

        std::string task_id = registry_.lookup_task_id(f.task_id);
        if (task_id.empty()) {
            reply.body = "unknown";
            return reply;
        }

        {
            registry_.cancel(task_id);
        }
        reply.body = "cancel-requested";
        return reply;
    }
    static const char* status_to_str_(TaskStatus s)
        {
            switch (s)
            {
            case TaskStatus::Pending:return "pending";
            case TaskStatus::Processing:return "processing";
            case TaskStatus::Done:return "done";
            case TaskStatus::Failed:return "failed";
            }return "unknown";
        }
    private:
    WAL& wal_;
    PriorityScheduler& scheduler_;
    TaskRegistry& registry_;
    
};
