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

using PageId = int32_t;
constexpr PageId INVALID_PAGE_ID = -1;

using FrameId = int32_t;
constexpr FrameId INVALID_FRAME_ID = -1;

constexpr int kRPCWorkerNum = 4;
constexpr int kOneSideWorkerNum = 16;

constexpr int kPoolHashSeed = 0x89ea7d2f;

#ifdef TEST_CONFIG
constexpr int kPoolShardingBits = 1;  // for test

#else
constexpr int kPoolShardingBits = 7;
#endif
constexpr int kPoolShardingNum = 1 << kPoolShardingBits;  // sharding num

constexpr int kKeyLength = 16;
constexpr int kSlabSize = 16;

#ifdef TEST_CONFIG
constexpr int kPageSizeBit = 10;  // 1KB
#else
constexpr int kPageSizeBit = 14;  // 16KB
#endif

constexpr int kPageSize = 1 << kPageSizeBit;

constexpr int kSlabSizeMin = 5;   // 5 * 16 = 80 Bytes
constexpr int kSlabSizeMax = 64;  // 64 * 16 = 1024 Bytes

#ifdef TEST_CONFIG
constexpr size_t kKeyNum = 12 * 16 * 1024;              // 16 * 12K key
constexpr size_t kPoolSize = (size_t)32 * 1024 * 1024;  // 32MB remote pool
constexpr size_t kBufferPoolSize = 2 * 1024 * 1024;     // 2MB cache
#else

constexpr size_t kKeyNum = 12 * 16 * 1024 * 1024;                   // 16 * 12M key
constexpr size_t kPoolSize = (size_t)28 * 1024 * 1024 * 1024;       // 32GB remote pool
constexpr size_t kBufferPoolSize = (size_t)2 * 1024 * 1024 * 1024;  // 2GB cache
#endif

constexpr size_t kPoolShardingSize = kPoolSize / kPoolShardingNum;

using Addr = int32_t;

constexpr Addr INVALID_ADDR = (-1);
constexpr uint32_t PAGE_BIT = 16;  // MAX 64 * 1024
constexpr uint32_t PAGE_MASK = 0xffff;
constexpr uint32_t OFF_BIT = 12;  // MAX 4096
constexpr uint32_t OFF_MASK = 0xfff;

class AddrParser {
 public:
  static kv::PageId PageId(Addr addr) { return (addr >> OFF_BIT) & PAGE_MASK; }
  static uint32_t Off(Addr addr) { return addr & OFF_MASK; }
  static Addr GenAddrFrom(kv::PageId id, uint32_t off) {
    assert((uint32_t)id < PAGE_MASK);
    assert(off < OFF_MASK);
    return (id << OFF_BIT) | off;
  }
};

}  // namespace kv