#include "assert.h"
#include "kv_engine.h"

#define MAX_VALUE_SIZE 4096

namespace kv {

/**
 * @description: start local engine service
 * @param {string} addr    the address string of RemoteEngine to connect
 * @param {string} port   the port of RemoteEngine to connect
 * @return {bool} true for success
 */
bool LocalEngine::start(const std::string addr, const std::string port) {
  m_rdma_conn_ = new ConnectionManager();
  if (m_rdma_conn_ == nullptr) return -1;
  if (m_rdma_conn_->init(addr, port, 4, 16))
    return false;
  else
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
  assert(m_rdma_conn_ != nullptr);
  internal_value_t internal_value;
  uint64_t remote_addr;
  uint32_t rkey;
  int ret = 0;
  ret = m_rdma_conn_->register_remote_memory(remote_addr, rkey, value.size());
  if (ret) return false;
  ret = m_rdma_conn_->remote_write((void *)value.c_str(), value.size(),
                                   remote_addr, rkey);
  if (ret) return false;
  internal_value.remote_addr = remote_addr;
  internal_value.rkey = rkey;
  internal_value.size = value.size();
  m_mutex_.lock();
  m_map_[key] = internal_value;
  m_mutex_.unlock();
  return true;
}

/**
 * @description: read value from engine via key
 * @param {string} key
 * @param {string} &value
 * @return {bool}  true for success
 */
bool LocalEngine::read(const std::string key, std::string &value) {
  internal_value_t inter_val;
  m_mutex_.lock();
  auto it = m_map_.find(key);
  if (it == m_map_.end()) {
    m_mutex_.unlock();
    return false;
  }
  inter_val = it->second;
  m_mutex_.unlock();
  value.reserve(inter_val.size);
  if (m_rdma_conn_->remote_read((void *)value.c_str(), inter_val.size,
                                inter_val.remote_addr, inter_val.rkey))
    return false;
  return true;
}

}  // namespace kv