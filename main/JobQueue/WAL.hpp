#pragma once

#include <fstream>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "Task.hpp"

class WAL {
public:
    explicit WAL(std::string path)
        : path_(std::move(path)) {}

    void append(const Task& task) const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::ofstream out(path_, std::ios::app);
        if (!out)
            throw std::runtime_error("failed to open WAL for append: " + path_);

        out << escape_(task.id) << '\t'
            << escape_(task.type) << '\t'
            << escape_(task.payload) << '\t'
            << task.priority << '\t'
            << task.created_at << '\t'
            << task.retries << '\t'
            << status_to_int_(task.status) << '\n';
        if (!out)
            throw std::runtime_error("failed to write WAL: " + path_);
    }

    std::vector<Task> replay() const {
        std::ifstream in(path_);
        if (!in)
            return {};

        std::vector<Task> tasks;
        std::string line;
        while (std::getline(in, line)) {
            try {
                auto task = parse_line_(line);
                if (task.has_value())
                    tasks.push_back(std::move(task.value()));
            } catch (const std::exception&) {
                // A crash can leave an incomplete final record; keep valid history.
            }
        }
        return tasks;
    }

    const std::string& path() const {
        return path_;
    }

private:
    static int status_to_int_(TaskStatus status) {
        switch (status) {
            case TaskStatus::Pending: return 0;
            case TaskStatus::Processing: return 1;
            case TaskStatus::Done: return 2;
            case TaskStatus::Failed: return 3;
        }
        return 0;
    }

    static TaskStatus int_to_status_(int status) {
        switch (status) {
            case 1: return TaskStatus::Processing;
            case 2: return TaskStatus::Done;
            case 3: return TaskStatus::Failed;
            default: return TaskStatus::Pending;
        }
    }

    static std::string escape_(const std::string& input) {
        std::string out;
        out.reserve(input.size());
        for (char ch : input) {
            switch (ch) {
                case '\\': out += "\\\\"; break;
                case '\n': out += "\\n"; break;
                case '\t': out += "\\t"; break;
                default: out += ch; break;
            }
        }
        return out;
    }

    static std::string unescape_(const std::string& input) {
        std::string out;
        out.reserve(input.size());
        bool escaped = false;
        for (char ch : input) {
            if (!escaped) {
                if (ch == '\\') {
                    escaped = true;
                } else {
                    out += ch;
                }
                continue;
            }

            switch (ch) {
                case 'n': out += '\n'; break;
                case 't': out += '\t'; break;
                default: out += ch; break;
            }
            escaped = false;
        }
        if (escaped)
            out += '\\';
        return out;
    }

    static std::vector<std::string> split_(const std::string& line) {
        std::vector<std::string> fields;
        std::string current;
        bool escaped = false;

        for (char ch : line) {
            if (!escaped && ch == '\t') {
                fields.push_back(current);
                current.clear();
                continue;
            }

            if (!escaped && ch == '\\') {
                escaped = true;
            } else {
                escaped = false;
            }
            current += ch;
        }

        fields.push_back(current);
        return fields;
    }

    static std::optional<Task> parse_line_(const std::string& line) {
        auto fields = split_(line);
        if (fields.size() != 7)
            return std::nullopt;

        Task task;
        task.id = unescape_(fields[0]);
        task.type = unescape_(fields[1]);
        task.payload = unescape_(fields[2]);
        task.priority = std::stoi(fields[3]);
        task.created_at = std::stoll(fields[4]);
        task.retries = std::stoi(fields[5]);
        task.status = int_to_status_(std::stoi(fields[6]));
        return task;
    }

    std::string path_;
    mutable std::mutex mutex_;
};
