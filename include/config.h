#pragma once
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include "util/logging.h"
#include "util/rwlock.h"

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

// 哈希种子
constexpr int kPoolHashSeed = 0x89ea7d2f;

// Pool的数量，对应的bit数目以及掩码
constexpr int kPoolShardingBits = 0;
constexpr int kPoolShardingMask = (1 << kPoolShardingBits) - 1;
constexpr int kPoolShardingNum = 1 << kPoolShardingBits;  // sharding num

// key的长度16字节
constexpr int kKeyLength = 16;
// 一个slab的大小为16字节，这是内存管理的最小单位
constexpr int kSlabSize = 16;
// 配置PageSize的大小
constexpr int kPageSizeBit = 20;  // 20: 1MB
constexpr int kPageSize = 1 << kPageSizeBit;
constexpr int kNumOfKB = 1024;

// page管理的一个slot的大小最小为5个slab，也就是80字节
constexpr int kSlabSizeMin = 5;  // 5 * 16 = 80 Bytes
// page管理的一个slot的大小最大为64个slab，也就是1024字节
constexpr int kSlabSizeMax = 64;  // 64 * 16 = 1024 Bytes
// 正在分配的页的分片数量，目前每个线程管理一个，因此配置为2^4=16个分片
constexpr int kAllocingListShardBit = 4;
constexpr int kAllocingListShard = 1 << kAllocingListShardBit;
constexpr int kAllocingListShardMask = kAllocingListShard - 1;

// key的数目
constexpr size_t kKeyNum = 12 * 16 * 1024 * 1024;  // 16 * 12M key
// remote pool的大小
constexpr size_t kPoolSize = (size_t)30 * 1024 * 1024 * 1024;  // 30GB remote pool
// cache的大小
constexpr size_t kBufferPoolSize = (size_t)4 * 1024 * 1024 * 1024;  // 4GB cache
// remote pool一个mr的大小
constexpr size_t kMaxBlockSize = (size_t)1 * 1024 * 1024 * 1024;      // 1GB mr
constexpr size_t kMaxPoolBlockSize = (size_t)1 * 1024 * 1024 * 1024;  // 1GB mr
// remote pool mrblock的个数
constexpr int kMrBlockNum = kPoolSize / kMaxBlockSize;
constexpr int kPoolMrBlockNum = kBufferPoolSize / kMaxPoolBlockSize;
constexpr int kHashIndexSize = 201326611;

// read时预取的page个数
constexpr int kPrefetchPageNum = 4;
constexpr int kPrefetchLinerProbePageNum = 8;

constexpr int kPreFlushPageNum = 4;
// net buffer的相关配置项
constexpr int kNetBufferPageNum = 63;
constexpr int kRemoteThreadWorkNum = 4;

// RDMA相关参数配置
constexpr int MAX_BATCH_NUM = 128;
constexpr int MAX_CQE = MAX_BATCH_NUM;
constexpr int MAX_QP_WR = MAX_BATCH_NUM;
constexpr int MAX_CQ = MAX_BATCH_NUM;
constexpr int MAX_INITIATOR_DEPTH = 128;

using Addr = int32_t;
constexpr Addr INVALID_ADDR = (-1);

// 编址相关的常量
// 2^PGAE_BIT = 32GB / kPageSize
// SHIFT = log(kMrBlockNum)
constexpr int kShift = 5;
constexpr uint32_t PAGE_BIT = 35 - kPageSizeBit;
constexpr uint32_t PAGE_MASK = (1 << PAGE_BIT) - 1;
constexpr uint32_t PAGE_OFF_MASK = (1 << (PAGE_BIT - kShift)) - 1;
constexpr uint32_t OFF_BIT = 32 - PAGE_BIT;  // MAX 4096
constexpr uint32_t OFF_MASK = (1 << OFF_BIT) - 1;
constexpr int kThreadNum = 16;

class AddrParser {
  static_assert(kPageSizeBit - 6 < OFF_BIT);

 public:
  static kv::PageId PageId(Addr addr) { return (addr >> OFF_BIT) & PAGE_MASK; }
  static uint32_t Off(Addr addr) { return addr & OFF_MASK; }
  static Addr GenAddrFrom(kv::PageId id, uint32_t off) {
    assert((uint32_t)id < PAGE_MASK);
    assert(off < OFF_MASK);
    return (id << OFF_BIT) | off;
  }
  static uint32_t GetBlockFromPageId(kv::PageId page_id) { return page_id >> (PAGE_BIT - kShift); }
  static uint32_t GetBlockOffFromPageId(kv::PageId page_id) { return page_id & PAGE_OFF_MASK; }
};

// 全局变量
constexpr size_t TOTAL_PAGE_NUM = kPoolSize / kPageSize;
extern SpinLock page_locks_[TOTAL_PAGE_NUM];
class PageEntry;
struct _Result {
  volatile bool _done;
  PageEntry *_result;
};
extern std::shared_ptr<_Result> _do[TOTAL_PAGE_NUM];
extern thread_local int cur_thread_id;
extern bool open_compress;
extern bool open_prefetch;
}  // namespace kv