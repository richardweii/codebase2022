#pragma once
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include "util/nocopy.h"

namespace kv {
#define RDMA_MR_FLAG (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE)
#define TIME_NOW (std::chrono::high_resolution_clock::now())
#define TIME_DURATION_US(START, END) (std::chrono::duration_cast<std::chrono::microseconds>((END) - (START)).count())
#define RESOLVE_TIMEOUT_MS 5000
#define RDMA_TIMEOUT_US 10000000  // 10s
#define RDMA_BATCH_US 100         // 100us

constexpr int kRPCWorkerNum = 4;
constexpr int kRDMABatchNum = 4;
constexpr bool kRDMASync = false;
constexpr int kOneSideWorkerNum = 16;

constexpr int kPoolHashSeed = 0x89ea7d2f;

constexpr int kPoolShardBits = 5;
// constexpr int kPoolShardBits = 1;
constexpr int kPoolShardNum = 1 << kPoolShardBits;

// for test
// constexpr size_t kLocalDataSize = (size_t)4 * 16 * 1024;  // 128KB = 16 * 8
// constexpr size_t kKeyNum = 16 * 100;                      // 16 * 12 * 32K key
// constexpr size_t kCacheSize = 2 * 16 * 1024;                  // 32KB cache

constexpr size_t kLocalDataSize = (size_t)4 * 1024 * 1024 * 1024;  // 4GB local data
constexpr size_t kKeyNum = 12 * 16 * 1024 * 1024;                  // 16 * 12M key
constexpr size_t kCacheSize = (size_t)2 * 1024 * 1024 * 1024;      // 2GB cache

constexpr int kKeyLength = 16;
constexpr int kValueLength = 128;
constexpr int kBloomFilterBitsPerKey = 10;
constexpr int kDataBlockSize = 32 * 1024 * 1024;  // 4MB
// constexpr int kDataBlockSize = 2 * 1024;  // 16KB
constexpr int kItemNum = kDataBlockSize * 8 / (kKeyLength * 8 + kValueLength * 8 + kBloomFilterBitsPerKey);
constexpr int kDataSize = kItemNum * (kKeyLength + kValueLength);
constexpr int kFilterSize = (kItemNum * kBloomFilterBitsPerKey + 7) / 8;

constexpr int kAlign = 8;  // kAlign show be powers of 2, say 2, 4 ,8, 16, 32, ...
inline constexpr int roundUp(unsigned int nBytes) { return ((nBytes) + (kAlign - 1)) & ~(kAlign - 1); }

constexpr int kRemoteMrSize = 64 * 1024 * 1024;
// constexpr int kRemoteMrSize = 16 * 1024;

template <typename Tp>
using Ptr = Tp *;

using BlockId = int32_t;

constexpr BlockId INVALID_BLOCK_ID = 0;

using FrameId = int32_t;
constexpr FrameId INVALID_FRAME_ID = -1;

}  // namespace kv