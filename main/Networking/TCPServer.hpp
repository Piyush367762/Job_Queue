#pragma once

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
#include "Session.hpp"
#include "SocketPlatform.hpp"
#include "SocketUtils.hpp"
//1)accept connections
//2) read bytes
//3)decode frame and call something to produce a reply
class TCPServer {
public:
    explicit TCPServer(uint16_t port, FrameHandler handler)
        : handler_(std::move(handler))
        , listen_fd_(invalid_socket_handle)
        , running_(false)
        , port_(port)
        , connections_(0) {
        ensure_socket_runtime();
    }

    ~TCPServer() {
        stop();
    }

    void start() {
        bool expected = false;
        if (!running_.compare_exchange_strong(expected, true))
            return;

        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ == invalid_socket_handle) {
            running_ = false;
            throw std::runtime_error(socket_error_message("socket"));
        }

        int opt = 1;
        ::setsockopt(listen_fd_,
                     SOL_SOCKET,
                     SO_REUSEADDR,
                     reinterpret_cast<const char*>(&opt),
                     sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port_);

        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            auto msg = socket_error_message("bind");
            close_socket(listen_fd_);
            listen_fd_ = invalid_socket_handle;
            running_ = false;
            throw std::runtime_error(msg);
        }

        if (::listen(listen_fd_, 128) < 0) {
            auto msg = socket_error_message("listen");
            close_socket(listen_fd_);
            listen_fd_ = invalid_socket_handle;
            running_ = false;
            throw std::runtime_error(msg);
        }

        accept_thread_ = std::thread([this] { accept_loop(); });
    }

    void stop() {
        bool expected = true;
        if (!running_.compare_exchange_strong(expected, false))
            return;

        if (listen_fd_ != invalid_socket_handle) {
#ifdef _WIN32
            ::shutdown(listen_fd_, SD_BOTH);
#else
            ::shutdown(listen_fd_, SHUT_RDWR);
#endif
            close_socket(listen_fd_);
            listen_fd_ = invalid_socket_handle;
        }

        if (accept_thread_.joinable())
            accept_thread_.join();
    }

    uint16_t port() const { return port_; }
    uint64_t connections() const { return connections_; }

private:
    void accept_loop() {
        while (running_) {
            sockaddr_in client_addr{};
            socklen_t client_len = sizeof(client_addr);

            socket_handle conn_fd = ::accept(listen_fd_,
                                             reinterpret_cast<sockaddr*>(&client_addr),
                                             &client_len);
            if (conn_fd == invalid_socket_handle) {
                if (!running_) break;
                continue;
            }

            ++connections_;

            std::thread([conn_fd, this] {
                Session s(conn_fd, handler_);
                s.run();
            }).detach();
        }
    }

    FrameHandler          handler_;
    socket_handle         listen_fd_;
    std::atomic<bool>     running_;
    std::thread           accept_thread_;
    uint16_t              port_;
    std::atomic<uint64_t> connections_;
};
