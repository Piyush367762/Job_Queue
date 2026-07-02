#pragma once
#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "../Data_Struct/Circular_Buffer.hpp"
#include "PriorityScheduler.hpp"
#include "../Networking/Protocol.hpp"
#include "../Networking/TCPServer.hpp"
#include "Task.hpp"
#include "WAL.hpp"
#include "WorkerPool.hpp"
//it mimic the function of remote client with TCPServer

namespace jobq_client {

inline socket_handle connect_to(const char* host, uint16_t port) {
    ensure_socket_runtime();
    socket_handle fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, host, &addr.sin_addr);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close_socket(fd);
        return invalid_socket_handle;
    }
    return fd;
}

inline proto::Frame roundtrip(socket_handle fd, const proto::Frame& req) {
    auto wire = req.encode();
    if (!send_all(fd, wire))
        throw std::runtime_error("send failed");

    proto::Decoder dec;
    uint8_t buf[4096];
    while (true) {
        int n = ::recv(fd, reinterpret_cast<char*>(buf), sizeof(buf), 0);
        if (n <= 0) throw std::runtime_error("recv failed / connection closed");
        auto frame = dec.feed(buf, static_cast<std::size_t>(n));
        if (frame) return *frame;
    }
}

inline proto::Frame submit(socket_handle fd, int priority, const std::string& type,
                            const std::string& payload) {
    proto::Frame req;
    req.cmd = proto::Cmd::SUBMIT;
    req.body = std::to_string(priority) + "|" + type + "|" + payload;
    return roundtrip(fd, req);
}

inline proto::Frame status(socket_handle fd, uint64_t wire_id) {
    proto::Frame req;
    req.cmd = proto::Cmd::STATUS;
    req.task_id = wire_id;
    return roundtrip(fd, req);
}

} 