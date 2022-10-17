#include "stat.h"
#include <atomic>
namespace kv {
namespace stat {
std::atomic_int64_t cache_hit(0);
std::atomic_int64_t replacement(0);
std::atomic_int64_t read_times(0);
std::atomic_int64_t write_times(0);
std::atomic_int64_t delete_times(0);
std::atomic_int64_t read_miss(0);
std::atomic_int64_t insert_num(0);
std::atomic_int64_t dirty_write(0);
std::atomic_int64_t hit_net_buffer(0);
std::atomic_int64_t miss_net_buffer(0);

}  // namespace stat
}  // namespace kv