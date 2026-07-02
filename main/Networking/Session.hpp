#pragma once
#include "SocketPlatform.hpp"
#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include "Protocol.hpp"
#include "SocketUtils.hpp"
using FrameHandler = std::function<proto::Frame(const proto::Frame&)>;
class Session {
public:
    Session(socket_handle fd, FrameHandler handler)
        : fd_(fd), handler_(std::move(handler)), alive_(true) {
        int flag = 1;
        ::setsockopt(fd_,
                     IPPROTO_TCP,
                     TCP_NODELAY,
                     reinterpret_cast<const char*>(&flag),
                     sizeof(flag));
    }

    ~Session() {
        close_socket(fd_);
    }

    void run() {
        uint8_t buf[4096];
        while (alive_) {
            int n = ::recv(fd_, reinterpret_cast<char*>(buf), sizeof(buf), 0);
            if (n <= 0) break;

            try {
                auto frame = dec_.feed(buf, static_cast<std::size_t>(n));
                if (!frame) continue; //partial frame, wait for more bytes
                auto reply = handler_(*frame);//call into JobQueue via injected lamda
                //what if we dont have any reply ?
                auto wire = reply.encode();
                //what does this send_all do ?
                if (!send_all(fd_, wire)) break;
            } catch (const std::exception& e) {
                proto::Frame err;
                err.cmd = proto::Cmd::ERROR;
                err.body = std::string("protocol error: ") + e.what();
                send_all(fd_, err.encode());
                break;
            }
        }
    }

private:
    socket_handle  fd_ = invalid_socket_handle;
    FrameHandler   handler_;
    proto::Decoder dec_;
    bool           alive_;
};
