#pragma once
#include <cstdint>
#include <cstring>
#include <vector>
#include <string>
#include <stdexcept>
#include <optional>
#include <cstddef>

#ifdef ERROR
#undef ERROR
#endif
namespace proto {
 
// ── Command byte ──
enum class Cmd : uint8_t {
    SUBMIT   = 0x01,
    STATUS   = 0x02,
    CANCEL   = 0x03,
    ACK      = 0x81,
    RESULT   = 0x82,
    ERROR    = 0x83,
    STATUS_R = 0x84,
};
 
inline const char* cmd_name(Cmd c) {
    switch (c) {
        case Cmd::SUBMIT:   return "SUBMIT";
        case Cmd::STATUS:   return "STATUS";
        case Cmd::CANCEL:   return "CANCEL";
        case Cmd::ACK:      return "ACK";
        case Cmd::RESULT:   return "RESULT";
        case Cmd::ERROR:    return "ERROR";
        case Cmd::STATUS_R: return "STATUS_R";
        default:            return "UNKNOWN";
    }
}
 
static constexpr uint32_t MAGIC      = 0x4A4F4251u;  // "JOBQ"
static constexpr std::size_t HDR_LEN = 17;            // magic(4)+cmd(1)+id(8)+len(4)
 
// ── LE helpers ──
//uint8_t 4 byte
//uint32_t 32 byte
inline void w32(uint8_t* p, uint32_t v){
    p[0]=v;p[1]=v>>8;p[2]=v>>16;p[3]=v>>24;
}
//(00000010)(00001101)(00000001)(00000011)
//p[0]=(00000011) or (p+0)*
//p[1]=(00000001) after shifting 8 time 
inline void w64(uint8_t* p, uint64_t v){
    for(int i=0;i<8;++i,v>>=8) p[i]=uint8_t(v);
}
inline uint32_t r32(const uint8_t* p){
    return uint32_t(p[0])|(uint32_t(p[1])<<8)|
           (uint32_t(p[2])<<16)|(uint32_t(p[3])<<24);
}
inline uint64_t r64(const uint8_t* p){
    uint64_t v=0;for(int i=7;i>=0;--i)v=(v<<8)|p[i];return v;
} 
struct Frame {
    Cmd         cmd     = Cmd::SUBMIT;
    uint64_t    task_id = 0;
    std::string body;           // UTF-8 string or raw bytes
 
    // Encode to wire bytes
    std::vector<uint8_t> encode() const {
        uint32_t blen = static_cast<uint32_t>(body.size());
        std::vector<uint8_t> buf(HDR_LEN + blen);
        uint8_t* p = buf.data();
        w32(p,     MAGIC);
        p[4] = static_cast<uint8_t>(cmd);
        w64(p + 5, task_id);
        w32(p + 13, blen);
        std::memcpy(p + HDR_LEN, body.data(), blen);
        return buf;
    }
};
 
// ── Incremental decoder ──
//  Feed raw bytes from recv().  Call decode() after each chunk.
//  Returns a complete Frame when one arrives.
class Decoder {
public:
    // Returns decoded frame if one is complete, nullopt otherwise.
    std::optional<Frame> feed(const uint8_t* data, std::size_t len) {
        buf_.insert(buf_.end(), data, data + len);
        return try_decode();
    }
 
    std::optional<Frame> feed(const std::vector<uint8_t>& chunk) {
        return feed(chunk.data(), chunk.size());
    }
 
    // Reset after a protocol error
    void reset() { buf_.clear(); }
 
private:
    std::vector<uint8_t> buf_;
    //it stored 8 byte of number in the array
    std::optional<Frame> try_decode() {
        if (buf_.size() < HDR_LEN) return std::nullopt;
 
        const uint8_t* p = buf_.data();
 
        // Magic check
        if (r32(p) != MAGIC) {
            reset();
            throw std::runtime_error("Protocol: bad magic number");
        }
 
        uint32_t blen = r32(p + 13);
        std::size_t total = HDR_LEN + blen;
        if (buf_.size() < total) return std::nullopt;   // incomplete
        //decode into cmd and task_id and body
        Frame f;
        f.cmd     = static_cast<Cmd>(p[4]);
        f.task_id = r64(p + 5);
        f.body.assign(reinterpret_cast<const char*>(p + HDR_LEN), blen);
 
        // Consume from buffer
        buf_.erase(buf_.begin(), buf_.begin() + static_cast<std::ptrdiff_t>(total));
        return f;
    }
};
 
}
