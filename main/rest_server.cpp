#include <chrono>
#include <cctype>
#include <iostream>
#include <sstream>
#include <thread>
#include <unordered_map>

#include "JobQueue/Jobqueueserver.hpp"
#include "Networking/HttpServer.hpp"

static std::string url_decode(const std::string& input) {
    std::string output;
    for (std::size_t i = 0; i < input.size(); ++i) {
        if (input[i] == '+') {
            output += ' ';
        } else if (input[i] == '%' && i + 2 < input.size()) {
            auto hex = [](char c) -> int {
                if (c >= '0' && c <= '9') return c - '0';
                if (c >= 'a' && c <= 'f') return c - 'a' + 10;
                if (c >= 'A' && c <= 'F') return c - 'A' + 10;
                return -1;
            };
            int high = hex(input[i + 1]);
            int low = hex(input[i + 2]);
            if (high >= 0 && low >= 0) {
                output += static_cast<char>((high << 4) | low);
                i += 2;
            } else {
                output += input[i];
            }
        } else {
            output += input[i];
        }
    }
    return output;
}

static std::unordered_map<std::string, std::string> form_fields(const std::string& body) {
    std::unordered_map<std::string, std::string> fields;
    std::size_t start = 0;
    while (start <= body.size()) {
        auto end = body.find('&', start);
        auto part = body.substr(start, end == std::string::npos ? end : end - start);
        auto equals = part.find('=');
        if (equals != std::string::npos)
            fields[url_decode(part.substr(0, equals))] = url_decode(part.substr(equals + 1));
        if (end == std::string::npos) break;
        start = end + 1;
    }
    return fields;
}

static std::string json_escape(const std::string& input) {
    std::string output;
    for (char c : input) {
        if (c == '"' || c == '\\') output += '\\';
        output += c;
    }
    return output;
}

static HttpResponse json(int status, std::string body) {
    return HttpResponse{status, std::move(body), "application/json"};
}

int main(int argc, char** argv) {
    uint16_t tcp_port = argc > 1 ? static_cast<uint16_t>(std::stoi(argv[1])) : 9190;
    uint16_t http_port = argc > 2 ? static_cast<uint16_t>(std::stoi(argv[2])) : 8080;
    std::string wal_path = argc > 3 ? argv[3] : "rest.wal";

    JobQueueServer server(tcp_port, 4, 4, wal_path);
    server.start();

    HttpServer http(http_port, [&server](const std::string& method,
                                         const std::string& target,
                                         const std::string& body) -> HttpResponse {
        if (method == "GET" && target == "/health")
            return json(200, "{\"status\":\"ok\"}");

        if (method == "GET" && target == "/metrics") {
            auto stats = server.live_stats();
            std::ostringstream out;
            out << "{\"scheduler_queued\":" << stats.scheduler_queued
                << ",\"pool_queued\":" << stats.pool_queued
                << ",\"pool_active\":" << stats.pool_active
                << ",\"done\":" << stats.done_count
                << ",\"failed\":" << stats.failed_count
                << ",\"dead_lettered\":" << stats.dead_letter_count
                << "}";
            return json(200, out.str());
        }

        if (method == "POST" && target == "/jobs") {
            auto fields = form_fields(body);
            int priority = fields.count("priority") ? std::stoi(fields["priority"]) : 5;
            std::string type = fields.count("type") ? fields["type"] : "generic";
            std::string payload = fields.count("payload") ? fields["payload"] : "";
            proto::Frame request;
            request.cmd = proto::Cmd::SUBMIT;
            request.body = std::to_string(priority) + "|" + type + "|" + payload;
            auto reply = server.handle_frame(request);
            return json(201, "{\"job_id\":" + std::to_string(reply.task_id) +
                        ",\"internal_id\":\"" + json_escape(reply.body) + "\"}");
        }

        const std::string prefix = "/jobs/";
        if (target.rfind(prefix, 0) == 0) {
            auto suffix = target.substr(prefix.size());
            bool cancel = suffix.size() > 8 && suffix.substr(suffix.size() - 8) == "/cancel";
            if (cancel) suffix.resize(suffix.size() - 8);
            uint64_t wire_id = 0;
            try { wire_id = std::stoull(suffix); }
            catch (...) { return json(400, "{\"error\":\"invalid job id\"}"); }

            proto::Frame request;
            request.task_id = wire_id;
            request.cmd = cancel ? proto::Cmd::CANCEL : proto::Cmd::STATUS;
            auto reply = server.handle_frame(request);
            if (cancel)
                return json(200, "{\"job_id\":" + std::to_string(wire_id) +
                            ",\"message\":\"" + json_escape(reply.body) + "\"}");
            return json(reply.body == "unknown" ? 404 : 200,
                        "{\"job_id\":" + std::to_string(wire_id) +
                        ",\"status\":\"" + json_escape(reply.body) + "\"}");
        }

        return json(404, "{\"error\":\"route not found\"}");
    });

    http.start();
    std::cout << "[rest_server] TCP port=" << tcp_port
              << " HTTP port=" << http_port << "\n";
    while (true)
        std::this_thread::sleep_for(std::chrono::seconds(1));
}
