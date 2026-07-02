#pragma once
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#ifdef ERROR
#undef ERROR
#endif
using socket_handle = SOCKET;
static constexpr socket_handle invalid_socket_handle = INVALID_SOCKET;
static constexpr int send_flags = 0;

inline void ensure_socket_runtime() {
    struct Runtime {
        Runtime() {
            WSADATA data{};
            int rc = WSAStartup(MAKEWORD(2, 2), &data);
            if (rc != 0)
                throw std::runtime_error("WSAStartup failed with error " + std::to_string(rc));
        }
    };
    static Runtime runtime;
    (void)runtime;
}

inline std::string socket_error_message(const char* call) {
    return std::string(call) + " failed with WSA error " + std::to_string(WSAGetLastError());
}

inline void close_socket(socket_handle fd) {
    if (fd != invalid_socket_handle)
        ::closesocket(fd);
}
#else
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
using socket_handle = int;
static constexpr socket_handle invalid_socket_handle = -1;
static constexpr int send_flags = MSG_NOSIGNAL;

inline void ensure_socket_runtime() {}

inline std::string socket_error_message(const char* call) {
    return std::string(call) + "(): " + std::strerror(errno);
}

inline void close_socket(socket_handle fd) {
    if (fd != invalid_socket_handle)
        ::close(fd);
}
#endif