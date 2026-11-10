#pragma once
#include "SocketPlatform.hpp"
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <utility>
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
                while (frame) {
                    auto reply = handler_(*frame);
                    if (!send_all(fd_, reply.encode())) {
                        alive_ = false;
                        break;
                    }
                    frame = dec_.next();
                }
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
