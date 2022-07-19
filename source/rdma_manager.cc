#include "rdma_manager.h"
#include <infiniband/verbs.h>
#include <netdb.h>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <thread>
#include "config.h"
#include "msg.h"
#include "msg_buf.h"
#include "util/logging.h"

namespace kv {

ibv_mr *RDMAManager::RegisterMemory(void *ptr, size_t length) {
  auto mr = ibv_reg_mr(pd_, ptr, length, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
  if (mr == nullptr) {
    perror("ibv_reg_mr failed : ");
    LOG_ERROR("Failed to register %zu bytes memory.", length);
    return nullptr;
  }
  return mr;
}

int RDMAManager::RemoteRead(void *ptr, uint32_t lkey, size_t size, uint64_t remote_addr, uint32_t rkey) {
  auto conn = rdma_one_side_->Dequeue();
  assert(conn != nullptr);
  auto ret = conn->RemoteRead(ptr, lkey, size, remote_addr, rkey);
  assert(ret == 0);
  rdma_one_side_->Enqueue(conn);
  return ret;
}

int RDMAManager::RemoteWrite(void *ptr, uint32_t lkey, size_t size, uint64_t remote_addr, uint32_t rkey) {
  auto conn = rdma_one_side_->Dequeue();
  assert(conn != nullptr);
  auto ret = conn->RemoteWrite(ptr, lkey, size, remote_addr, rkey);
  assert(ret == 0);
  rdma_one_side_->Enqueue(conn);
  return ret;
}

}  // namespace kv