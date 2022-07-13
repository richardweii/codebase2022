#pragma once
#include <chrono>

namespace kv {

#define TIME_NOW (std::chrono::high_resolution_clock::now())
#define TIME_DURATION_US(START, END) (std::chrono::duration_cast<std::chrono::microseconds>((END) - (START)).count())
#define RESOLVE_TIMEOUT_MS 5000
#define RDMA_TIMEOUT_US 1000000000  // 10s
// #define RDMA_TIMEOUT_US 10000000  // 10s
#define RDMA_BATCH_US 100         // 1ms

constexpr int kOneSideConnectionNum = 16;
constexpr int kWorkerNum = 4;
// constexpr int kSignalNum = 4;       // 每隔一定数目的 WQ才轮询一次CQ
constexpr int kRDMABatchNum = 4;

}  // namespace kv