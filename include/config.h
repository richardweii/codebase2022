#pragma once
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

// #define TEST_CONFIG  // test configuration marco switch
// #define STAT         // statistic

namespace kv {
#define RDMA_MR_FLAG (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE)
#define TIME_NOW (std::chrono::high_resolution_clock::now())
#define TIME_DURATION_US(START, END) (std::chrono::duration_cast<std::chrono::microseconds>((END) - (START)).count())
#define RESOLVE_TIMEOUT_MS 5000
#define RDMA_TIMEOUT_US 10000000  // 10s
#define RDMA_BATCH_US 100         // 100us

using BlockId = int32_t;
constexpr BlockId INVALID_BLOCK_ID = -1;

using FrameId = int32_t;
constexpr FrameId INVALID_FRAME_ID = -1;

constexpr int kRPCWorkerNum = 4;
constexpr int kOneSideWorkerNum = 16;

constexpr int kPoolHashSeed = 0x89ea7d2f;

#ifdef TEST_CONFIG
constexpr int kPoolShardBits = 1;  // for test

#else
constexpr int kPoolShardBits = 6;
// constexpr int kCacheShardBits = 4;
#endif
constexpr int kPoolShardNum = 1 << kPoolShardBits;

constexpr int kKeyLength = 16;
constexpr int kValueLength = 128;
constexpr int kValueBit = 7;

#ifdef TEST_CONFIG
constexpr int kValueBlockBit = 10 + 4;                // 16KB
constexpr int kValueBlockSize = 1 << kValueBlockBit;  // value memory register unit
constexpr int kCacheLineBit = 10;
constexpr int kCacheLineSize = 1 << kCacheLineBit;  // 1KB
#else
constexpr int kValueBlockBit = 27;                    //
constexpr int kValueBlockSize = 1 << kValueBlockBit;  // 128MB, value memory register unit
constexpr int kCacheLineBit = 17;
constexpr int kCacheLineSize = 1 << kCacheLineBit;             // 128KB
#endif

constexpr int kBlockValueNum = kValueBlockSize / kValueLength;
constexpr int kCacheValueNum = kCacheLineSize / kValueLength;

#ifdef TEST_CONFIG
constexpr size_t kKeyNum = 16 * 100;          // 16 * 12 * 32K key
constexpr size_t kCacheSize = 2 * 16 * 1024;  // 32KB cache
#else
constexpr size_t kKeyNum = 12 * 16 * 1024 * 1024;              // 16 * 12M key
constexpr size_t kCacheSize = (size_t)2 * 1024 * 1024 * 1024;  // 2GB cache
#endif

constexpr int kAlign = 8;  // kAlign show be powers of 2, say 2, 4 ,8, 16, 32, ...
inline constexpr int roundUp(unsigned int nBytes) { return ((nBytes) + (kAlign - 1)) & ~(kAlign - 1); }

constexpr uint32_t BLOCKID_MASK = uint32_t(~0) << (kValueBlockBit - kValueBit);
constexpr uint32_t CACHE_MASK = uint32_t(~0) << (kCacheLineBit - kValueBit);

class Addr {
 public:
  kv::BlockId BlockId() const { return addr_ >> (kValueBlockBit - kValueBit); }
  uint32_t CacheLine() const { return (addr_ & (~BLOCKID_MASK)) >> (kCacheLineBit - kValueBit); }
  uint32_t BlockOff() const { return addr_ & (~BLOCKID_MASK); }
  uint32_t CacheOff() const { return addr_ & (~CACHE_MASK); }

  void SetBlockId(kv::BlockId id) {
    assert(id <= (kv::BlockId)((uint32_t(~0) >> (kValueBlockBit - kValueBit))));
    addr_ |= (id < (kValueBlockBit - kValueBit));
  }
  void SetBlockOff(uint32_t off) {
    assert(off <= (~BLOCKID_MASK));
    addr_ |= (off & (~BLOCKID_MASK));
  }

  // round up cache line
  Addr RoundUp() {
    addr_ &= CACHE_MASK;
    return addr_;
  }

  uint32_t RawAddr() const { return addr_; }

  Addr() = default;
  Addr(kv::BlockId id, uint32_t off) { addr_ = (id << (kValueBlockBit - kValueBit)) | (off & (~BLOCKID_MASK)); }
  Addr(uint32_t key_index) { addr_ = key_index; }

  bool operator==(const Addr &addr) { return addr.addr_ == addr_; }

 private:
  uint32_t addr_ = 0;
};

}  // namespace kv