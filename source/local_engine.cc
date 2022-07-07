#include <cstddef>
#include "assert.h"
#include "config.h"
#include "kv_engine.h"
#include "pool.h"
#include "rdma_conn_manager.h"
#include "util/filter.h"
#include "util/hash.h"
#include "util/logging.h"

#define MAX_VALUE_SIZE 4096

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

  struct ibv_context **ibv_ctxs;
  int nr_devices_;
  ibv_ctxs = rdma_get_devices(&nr_devices_);
  if (!ibv_ctxs) {
    perror("get device list fail");
    return false;
  }

  auto context = ibv_ctxs[0];
  pd_ = ibv_alloc_pd(context);
  if (!pd_) {
    perror("ibv_alloc_pd fail");
    return false;
  }

  connection_manager_ = new ConnectionManager(pd_);
  connection_manager_->Init(addr, port, 4, 4);

  for (int i = 0; i < kPoolShardNum; i++) {
    pool_[i] = new Pool(buffer_pool_size, filter_bits, cache_size, i, connection_manager_);
    pool_[i]->Init();
  }
  return true;
}

/**
 * @description: stop local engine service
 * @return {void}
 */
void LocalEngine::stop() {
  // TODO
  return;
};

/**
 * @description: get engine alive state
 * @return {bool}  true for alive
 */
bool LocalEngine::alive() { return true; }

/**
 * @description: put a key-value pair to engine
 * @param {string} key
 * @param {string} value
 * @return {bool} true for success
 */
bool LocalEngine::write(const std::string key, const std::string value) {
  uint32_t hash = Hash(key.c_str(), key.size(), kPoolHashSeed);
  int index = Shard(hash);
  return pool_[index]->Write(Key(new std::string(key)), Value(new std::string(value)), NewBloomFilterPolicy());
}

/**
 * @description: read value from engine via key
 * @param {string} key
 * @param {string} &value
 * @return {bool}  true for success
 */
bool LocalEngine::read(const std::string key, std::string &value) {
  uint32_t hash = Hash(key.c_str(), key.size(), kPoolHashSeed);
  int index = Shard(hash);
  Value val = pool_[index]->Read(Key(new std::string(key)), NewBloomFilterPolicy());
  if (val != nullptr) {
    value = *val;
    return true;
  }
  return false;
}

}  // namespace kv