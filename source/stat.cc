#include "stat.h"
#include <atomic>
namespace kv {
namespace stat {
std::atomic_int64_t cache_hit(0);
std::atomic_int64_t cache_invalid(0);
std::atomic_int64_t replacement(0);
std::atomic_int64_t fetch(0);
std::atomic_int64_t read_times(0);
std::atomic_int64_t write_times(0);
std::atomic_int64_t local_access(0);
std::atomic_int64_t remote_miss(0);
std::atomic_int64_t insert_num(0);
std::atomic_int64_t block_num(0);

}  // namespace stat
}  // namespace kv