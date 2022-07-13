#include <cstring>
#include <iostream>
#include "assert.h"
#include "atomic"
#include "kv_engine.h"
#include "rdma_client.h"

namespace kv {

/**
 * @description: start local engine service
 * @param {string} addr    the address string of RemoteEngine to connect
 * @param {string} port   the port of RemoteEngine to connect
 * @return {bool} true for success
 */
bool LocalEngine::start(const std::string addr, const std::string port) {
  client_ = new RDMAClient();
  if (client_ == nullptr) return -1;
  if (!client_->Init(addr, port)) return false;
  m_rdma_mem_pool_ = new RDMAMemPool(client_);
  if (m_rdma_mem_pool_) return true;
  return false;
}

/**
 * @description: stop local engine service
 * @return {void}
 */
void LocalEngine::stop(){
    // TODO
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
  assert(client_ != nullptr);
  internal_value_t internal_value;
  uint64_t remote_addr;
  uint32_t rkey;

  int index = std::hash<std::string>()(key) & (SHARDING_NUM - 1);
  m_mutex_[index].lock();
  bool found = false;

  /* Use the corresponding shard hash map to look for key. */
  hash_map_slot *it = m_hash_map[index].find(key);

  if (it) {
    /* Reuse the old addr. In this case, the new value size should be the same
     * with old one. TODO: Solve the situation that the new value size is larger
     * than the old  one */
    remote_addr = it->internal_value.remote_addr;
    rkey = it->internal_value.rkey;
    found = true;
  } else {
    /* Not written yet, get the memory first. */
    if (m_rdma_mem_pool_->get_mem(value.size(), remote_addr, rkey)) {
      m_mutex_[index].unlock();
      return false;
    }
    // printf("get mem %lld %d\n", remote_addr, rkey);
  }
  m_mutex_[index].unlock();

  /* Optimization: local node could cache some hot data. No need to write the
   * data to the remote for each insertion. Moving local data to the remote
   * could be done in the background instead of the critical path. */
  /* Also, we can batch some KV pairs together, writing them to remote in one
   * RDMA write in the background */
  memset(local_buf_, 0, 128);
  memcpy(local_buf_, value.c_str(), value.size());
  int ret = client_->RemoteWrite(local_buf_, mr_->lkey, value.size(), remote_addr, rkey);
  if (ret) return false;
  // printf("write key: %s, value: %s, %lld %d\n", key.c_str(), value.c_str(),
  //        remote_addr, rkey);
  if (found) return true; /* no need to update hash map */

  /* Update the hash info. */
  internal_value.remote_addr = remote_addr;
  internal_value.rkey = rkey;
  internal_value.size = value.size();

  /* Optimization: To support concurrent insertion */
  m_mutex_[index].lock();

  /* Fetch a new slot from slot_array, do not need to new. */
  hash_map_slot *new_slot = &hash_slot_array[slot_cnt.fetch_add(1)];

  /* Update the hash_map. */
  m_hash_map[index].insert(key, internal_value, new_slot);

  m_mutex_[index].unlock();
  return true;
}

/**
 * @description: read value from engine via key
 * @param {string} key
 * @param {string} &value
 * @return {bool}  true for success
 */
bool LocalEngine::read(const std::string key, std::string &value) {
  int index = std::hash<std::string>()(key) & (SHARDING_NUM - 1);
  internal_value_t inter_val;
  m_mutex_[index].lock();
  hash_map_slot *it = m_hash_map[index].find(key);
  if (!it) {
    m_mutex_[index].unlock();
    return false;
  }
  inter_val = it->internal_value;
  m_mutex_[index].unlock();
  if (client_->RemoteRead(local_buf_, mr_->lkey, inter_val.size, inter_val.remote_addr, inter_val.rkey)) return false;
  value.resize(inter_val.size, '0');
  memcpy((char *)value.c_str(), local_buf_, inter_val.size);
  return true;
}

}  // namespace kv