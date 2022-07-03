#pragma once

#include "rdma_conn_manager.h"

#define RDMA_ALLOCATE_SIZE (1 << 20ul)

namespace kv {
class RDMAMemPool {
 public:
  typedef struct {
    uint64_t addr;
    uint32_t rkey;
  } rdma_mem_t;

  RDMAMemPool(ConnectionManager *conn_manager)
      : m_rdma_conn_(conn_manager), m_current_mem_(0), m_rkey_(0), m_pos_(0) {}

  ~RDMAMemPool() { destory(); }

  int get_mem(uint64_t size, uint64_t &addr, uint32_t &rkey);

 private:
  void destory();

  uint64_t m_current_mem_; /* current mem used for local allocation */
  uint32_t m_rkey_;        /* rdma remote key */
  uint64_t m_pos_;         /* the position used for allocation */
  std::vector<rdma_mem_t> m_used_mem_; /* the used mem */
  ConnectionManager *m_rdma_conn_;     /* rdma connection manager */
  std::mutex m_mutex_;                 /* used for concurrent mem allocation */
};
}  // namespace kv