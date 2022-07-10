#pragma once

#include <infiniband/verbs.h>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include "config.h"
#include "rdma_conn.h"
#include "util/logging.h"
#include "util/nocopy.h"

namespace kv {

/* The RDMA connection queue */
class ConnQue {
 public:
  ConnQue() {}
  ~ConnQue() {
    while (!m_queue_.empty()) {
      auto conn = m_queue_.front();
      delete conn;
      m_queue_.pop();
    }
  }
  void enqueue(RDMAConnection *conn) {
    std::unique_lock<std::mutex> lock(m_mutex_);
    m_queue_.push(conn);
  };

  RDMAConnection *dequeue() {
  retry:
    std::unique_lock<std::mutex> lock(m_mutex_);
    while (m_queue_.empty()) {
      lock.unlock();
      std::this_thread::yield();
      goto retry;
    }
    RDMAConnection *conn = m_queue_.front();
    m_queue_.pop();
    return conn;
  }

  bool empty() const { return m_queue_.empty(); }

 private:
  std::queue<RDMAConnection *> m_queue_;
  std::mutex m_mutex_;
};

/* The RDMA connection manager */
class ConnectionManager NOCOPYABLE {
 public:
  ConnectionManager(ibv_pd *pd) : pd_(pd){LOG_ASSERT(pd != nullptr, "Invalid pd.")};

  ~ConnectionManager() {
    while (!rpc_conn_queue_->empty()) {
      RDMAConnection *conn = rpc_conn_queue_->dequeue();
      assert(conn != nullptr);
      int ret = conn->Stop();
      delete conn;
    }
    delete one_sided_conn_queue_;
    delete rpc_conn_queue_;
  }
  ibv_pd *Pd() const { return pd_; }
  int Init(const std::string ip, const std::string port, uint32_t rpc_conn_num, uint32_t one_sided_conn_num);
  int Alloc(uint8_t shard, uint64_t &addr, uint32_t &rkey, uint64_t size);
  int Lookup(std::string key, uint64_t &addr, uint32_t &rkey, bool &found);
  int Free(uint8_t shard, BlockId id);
  int RemoteRead(void *ptr, uint32_t lkey, uint32_t size, uint64_t remote_addr, uint32_t rkey);
  int RemoteWrite(void *ptr, uint32_t lkey, uint32_t size, uint64_t remote_addr, uint32_t rkey);

 private:
  ConnQue *rpc_conn_queue_;
  ConnQue *one_sided_conn_queue_;
  ibv_pd *pd_;
};

};  // namespace kv