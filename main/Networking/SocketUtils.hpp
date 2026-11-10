#pragma once
#include <cstddef>
#include <limits>
#include <vector>
#include "SocketPlatform.hpp"

inline bool send_all(socket_handle fd, const std::vector<uint8_t>& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const auto remaining = data.size() - sent;
        const auto chunk = (remaining > static_cast<std::size_t>(std::numeric_limits<int>::max()))
            ? static_cast<std::size_t>(std::numeric_limits<int>::max()) : remaining;
        int n = ::send(fd,
                       reinterpret_cast<const char*>(data.data() + sent),
                       static_cast<int>(chunk),
                       send_flags);
        if (n <= 0) return false;
        sent += static_cast<std::size_t>(n);
    }
    return true;
}
