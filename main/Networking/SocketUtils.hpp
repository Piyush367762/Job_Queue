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

#include "Session.hpp"

inline bool send_all(socket_handle fd, const std::vector<uint8_t>& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        int n = ::send(fd,
                       reinterpret_cast<const char*>(data.data() + sent),
                       static_cast<int>(data.size() - sent),
                       send_flags);
        if (n <= 0) return false;
        sent += static_cast<std::size_t>(n);
    }
    return true;
}
