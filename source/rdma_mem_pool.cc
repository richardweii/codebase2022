#include "rdma_mem_pool.h"

namespace kv {

/* return 0 if success, otherwise fail to get the mem */
int RDMAMemPool::get_mem(uint64_t size, uint64_t &addr, uint32_t &rkey) {
  if (size > RDMA_ALLOCATE_SIZE) return -1;

  /* One of the optimizations is to support the concurrent mem allocation,
   * otherwise it could be the bottleneck */
  std::lock_guard<std::mutex> lk(m_mutex_);

retry:
  if (m_pos_ + size <= RDMA_ALLOCATE_SIZE &&
      m_current_mem_ != 0) { /* local mem is enough */
    addr = m_current_mem_ + m_pos_;
    rkey = m_rkey_;
    m_pos_ += size;
    return 0;
  }
  /* allocate mem from remote */

  /* 1. store the current mem to the used list */
  rdma_mem_t rdma_mem;
  rdma_mem.addr = m_current_mem_;
  rdma_mem.rkey = m_rkey_;
  m_used_mem_.push_back(rdma_mem);

  /* 2. allocate and register the new mem from remote */
  /* Another optimization is to move this remote memory registration to the
   * backgroud instead of using it in the critical path */
  int ret = m_rdma_conn_->register_remote_memory(m_current_mem_, m_rkey_,
                                                 RDMA_ALLOCATE_SIZE);
  // printf("allocate mem %lld %ld\n", m_current_mem_, m_rkey_);
  if (ret) return -1;
  m_pos_ = 0;
  /* 3. try to allocate again */
  goto retry;
}

void RDMAMemPool::destory() {
  // TODO: release allocated resources
}

}  // namespace kv