#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include "assert.h"
#include "atomic"
#include "config.h"
#include "hash_table.h"
#include "kv_engine.h"
#include "pool.h"
#include "rdma_client.h"
#include "stat.h"
#include "util/filter.h"
#include "util/get_clock.h"
#include "util/hash.h"
#include "util/logging.h"
#include "util/slice.h"

namespace kv {

/**
 * @description: start local engine service
 * @param {string} addr    the address string of RemoteEngine to connect
 * @param {string} port   the port of RemoteEngine to connect
 * @return {bool} true for success
 */
bool LocalEngine::start(const std::string addr, const std::string port) {
 constexpr size_t buffer_pool_size = kLocalDataSize / kPoolShardNum / kDataBlockSize;
  constexpr size_t filter_bits = 10 * kKeyNum / kPoolShardNum;
  constexpr size_t cache_size = kCacheSize / kPoolShardNum;
  LOG_INFO("Create %d pool, each pool with %lu datablock, %lu MB filter data, %lu MB cache", kPoolShardNum,
           buffer_pool_size, filter_bits / 8 / 1024 / 1024, cache_size / 1024 / 1024);
  client_ = new RDMAClient();
  if (!client_->Init(addr, port)) return false;
  client_->Start();

  for (int i = 0; i < kPoolShardNum; i++) {
    pool_[i] = new Pool(buffer_pool_size, filter_bits,cache_size, i, client_);
    pool_[i]->Init();
  }
  return true;
}

/**
 * @description: stop local engine service
 * @return {void}
 */
void LocalEngine::stop() {
  client_->Stop();
  delete client_;
  for (int i = 0; i < kPoolShardNum; i++) {
    delete pool_[i];
  }
  LOG_INFO(" ========== Performance Statistics ============");
  LOG_INFO(" Total read %ld times, write %ld times", stat::read_times.load(), stat::write_times.load());
  LOG_INFO(" Unique insert %ld  times", stat::insert_num.load());
  LOG_INFO(" Total block num %ld", stat::block_num.load());
  LOG_INFO(" Access to local %ld times ", stat::local_access.load());
  LOG_INFO(" Remote lookup failed %ld times", stat::remote_miss.load());
  LOG_INFO(" Replacement %ld times, Fetch %ld times ", stat::replacement.load(), stat::fetch.load());
  LOG_INFO(" Cache hit %ld times, invalid %ld times", stat::cache_hit.load(), stat::cache_invalid.load());
  LOG_INFO(" Read Miss %ld times", stat::read_miss.load());
  auto mhz = get_cpu_mhz(1);
  LOG_INFO(" CPU frequency %f MHZ", mhz);
  return;
};

/**
 * @description: get engine alive state
 * @return {bool}  true for alive
 */
bool LocalEngine::alive() { return client_->Alive(); }

/**
 * @description: put a key-value pair to engine
 * @param {string} key
 * @param {string} value
 * @return {bool} true for success
 */
bool LocalEngine::write(const std::string key, const std::string value) {
  stat::write_times.fetch_add(1, std::memory_order_relaxed);
  uint32_t hash = Hash(key.c_str(), key.size(), kPoolHashSeed);
  int index = Shard(hash);
  return pool_[index]->Write(Slice(key), Slice(value));
}

/**
 * @description: read value from engine via key
 * @param {string} key
 * @param {string} &value
 * @return {bool}  true for success
 */
bool LocalEngine::read(const std::string key, std::string &value) {
  stat::read_times.fetch_add(1, std::memory_order_relaxed);
  uint32_t hash = Hash(key.c_str(), key.size(), kPoolHashSeed);
  int index = Shard(hash);
  return pool_[index]->Read(Slice(key), value);
}

}  // namespace kv