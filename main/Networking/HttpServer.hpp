#pragma once

#include <atomic>
#include <cstddef>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "SocketPlatform.hpp"
#include "SocketUtils.hpp"

struct HttpResponse {
    int status = 200;
    std::string body;
    std::string content_type = "application/json";
};

using HttpHandler = std::function<HttpResponse(const std::string& method,
                                               const std::string& target,
                                               const std::string& body)>;

class HttpServer {
public:
    HttpServer(uint16_t port, HttpHandler handler)
        : port_(port), handler_(std::move(handler)) {
        ensure_socket_runtime();
        if (!handler_)
            throw std::invalid_argument("HttpServer requires a handler");
    }

    ~HttpServer() { stop(); }

    HttpServer(const HttpServer&) = delete;
    HttpServer& operator=(const HttpServer&) = delete;

    void start() {
        bool expected = false;
        if (!running_.compare_exchange_strong(expected, true))
            return;

        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ == invalid_socket_handle) {
            running_ = false;
            throw std::runtime_error(socket_error_message("http socket"));
        }

        int opt = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char*>(&opt), sizeof(opt));

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = INADDR_ANY;
        address.sin_port = htons(port_);

        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0 ||
            ::listen(listen_fd_, 64) < 0) {
            auto message = socket_error_message("http bind/listen");
            close_socket(listen_fd_);
            listen_fd_ = invalid_socket_handle;
            running_ = false;
            throw std::runtime_error(message);
        }

        accept_thread_ = std::thread([this] { accept_loop_(); });
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

private:
    static std::string status_text_(int status) {
        switch (status) {
            case 200: return "OK";
            case 201: return "Created";
            case 400: return "Bad Request";
            case 404: return "Not Found";
            case 405: return "Method Not Allowed";
            case 500: return "Internal Server Error";
            default: return "Response";
        }
    }

    static bool parse_content_length_(const std::string& headers, std::size_t& length) {
        const std::string key = "Content-Length:";
        auto position = headers.find(key);
        if (position == std::string::npos) {
            length = 0;
            return true;
        }
        position += key.size();
        while (position < headers.size() && headers[position] == ' ') ++position;
        try {
            length = static_cast<std::size_t>(std::stoul(headers.substr(position)));
        } catch (...) {
            return false;
        }
        return length <= 1024 * 1024;
    }

    void accept_loop_() {
        while (running_) {
            socket_handle client = ::accept(listen_fd_, nullptr, nullptr);
            if (client == invalid_socket_handle) {
                if (!running_) break;
                continue;
            }
            std::thread([this, client] { handle_client_(client); }).detach();
        }
    }

    void handle_client_(socket_handle client) {
        std::string request;
        char buffer[4096];
        std::size_t header_end = std::string::npos;
        while (request.size() < 1024 * 1024 && header_end == std::string::npos) {
            int received = ::recv(client, buffer, sizeof(buffer), 0);
            if (received <= 0) {
                close_socket(client);
                return;
            }
            request.append(buffer, static_cast<std::size_t>(received));
            header_end = request.find("\r\n\r\n");
        }

        HttpResponse response;
        if (header_end == std::string::npos) {
            response = {400, "{\"error\":\"invalid HTTP request\"}"};
        } else {
            const std::string headers = request.substr(0, header_end);
            std::istringstream request_line(headers);
            std::string method, target, version;
            request_line >> method >> target >> version;
            std::size_t content_length = 0;
            if (method.empty() || target.empty() || version.empty() ||
                !parse_content_length_(headers, content_length)) {
                response = {400, "{\"error\":\"invalid HTTP headers\"}"};
            } else {
                const std::size_t body_start = header_end + 4;
                while (request.size() < body_start + content_length) {
                    int received = ::recv(client, buffer, sizeof(buffer), 0);
                    if (received <= 0) break;
                    request.append(buffer, static_cast<std::size_t>(received));
                }
                if (request.size() < body_start + content_length) {
                    response = {400, "{\"error\":\"incomplete request body\"}"};
                } else {
                    try {
                        response = handler_(method, target,
                                            request.substr(body_start, content_length));
                    } catch (const std::exception& error) {
                        response = {500, std::string("{\"error\":\"") + error.what() + "\"}"};
                    }
                }
            }
        }

        const std::string wire =
            "HTTP/1.1 " + std::to_string(response.status) + " " + status_text_(response.status) + "\r\n" +
            "Content-Type: " + response.content_type + "\r\n" +
            "Content-Length: " + std::to_string(response.body.size()) + "\r\n" +
            "Connection: close\r\n\r\n" + response.body;
        send_all(client, std::vector<uint8_t>(wire.begin(), wire.end()));
        close_socket(client);
    }

    uint16_t port_;
    HttpHandler handler_;
    socket_handle listen_fd_ = invalid_socket_handle;
    std::atomic<bool> running_{false};
    std::thread accept_thread_;
};
