#pragma once

#include <atomic>

namespace kv {
namespace stat {

// some performance counter, used for tuning
extern std::atomic_int64_t cache_hit;
extern std::atomic_int64_t replacement;
extern std::atomic_int64_t read_times;
extern std::atomic_int64_t write_times;
extern std::atomic_int64_t dirty_write;
extern std::atomic_int64_t read_miss;
extern std::atomic_int64_t insert_num;
extern std::atomic_int64_t block_num;
}  // namespace stat
}  // namespace kv