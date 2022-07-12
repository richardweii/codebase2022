#pragma once

namespace kv {

#define TIME_NOW (std::chrono::high_resolution_clock::now())
#define TIME_DURATION_US(START, END) (std::chrono::duration_cast<std::chrono::microseconds>((END) - (START)).count())
#define RESOLVE_TIMEOUT_MS 5000
#define RDMA_TIMEOUT_US 10000000  // 10s

constexpr int kOneSideConnectionNum = 16;
constexpr int kWorkerNum = 4;

}  // namespace kv