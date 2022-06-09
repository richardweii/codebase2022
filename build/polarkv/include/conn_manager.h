#pragma once

#include <mutex>
#include <queue>
#include <thread>
#include "connection.h"

namespace rdma {

/* The RDMA connection queue */
class ConnQue {
 public:
  ConnQue() {}

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

 private:
  std::queue<RDMAConnection *> m_queue_;
  std::mutex m_mutex_;
};

/* The RDMA connection manager */
class ConnectionManager {
 public:
  ConnectionManager() {}

  ~ConnectionManager() {
    // TODO: release resouces;
  }

  int init(const char *ip, const char *port, uint32_t rpc_conn_num,
           uint32_t one_sided_conn_num);
  void close();
  int register_remote_memory(uint64_t &addr, uint32_t &rkey, uint64_t size);
  int register_remote_memory(uint64_t size, ibv_mr *tmp_mr);
  int remote_read(uint64_t src, uint64_t remote_addr, uint32_t size,
                  uint32_t rkey);
  int remote_write(uint64_t src, uint64_t remote_addr, uint32_t size,
                   uint32_t rkey);

  ibv_pd *get_pd() { return m_pd_; }

 private:
  ConnQue *m_rpc_conn_queue_;
  ConnQue *m_one_sided_conn_queue_;
  ibv_pd *m_pd_{nullptr};  // TODO: different pd.
};

};  // namespace rdma