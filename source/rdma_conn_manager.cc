#include "rdma_conn_manager.h"
#include "util/logging.h"

namespace kv {

int ConnectionManager::Init(const std::string ip, const std::string port, uint32_t rpc_conn_num,
                            uint32_t one_sided_conn_num) {
  rpc_conn_queue_ = new ConnQue();
  one_sided_conn_queue_ = new ConnQue();
  if (rpc_conn_num > MAX_SERVER_WORKER) {
    LOG_DEBUG(
        "max server worker is %d, rpc_conn_num is: %d, reset rpc_conn_num to "
        "%d\n",
        MAX_SERVER_WORKER, rpc_conn_num, MAX_SERVER_WORKER);
    rpc_conn_num = MAX_SERVER_WORKER;
  }

  for (uint32_t i = 0; i < rpc_conn_num; i++) {
    RDMAConnection *conn = new RDMAConnection(pd_, i);
    if (conn->Init(ip, port)) {
      // TODO: release resources
      LOG_FATAL("Failed to init rpc connection.");
      return -1;
    }
    rpc_conn_queue_->enqueue(conn);
  }

  for (uint32_t i = 0; i < one_sided_conn_num; i++) {
    RDMAConnection *conn = new RDMAConnection(pd_, i + rpc_conn_num);
    if (conn->Init(ip, port)) {
      // TODO: release resources
      LOG_FATAL("Failed to init one side read/write connection.");
      return -1;
    }
    one_sided_conn_queue_->enqueue(conn);
  }
  for (uint32_t i = 0; i < rpc_conn_num; i++) {
    auto conn = rpc_conn_queue_->dequeue();
    conn->Ping();
    rpc_conn_queue_->enqueue(conn);
  }
  return 0;
}

int ConnectionManager::Alloc(uint8_t shard, uint64_t &addr, uint32_t &rkey, uint64_t size) {
  RDMAConnection *conn = rpc_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->AllocDataBlock(shard, addr, rkey);
  rpc_conn_queue_->enqueue(conn);
  return ret;
}

int ConnectionManager::RemoteRead(void *ptr, uint32_t lkey, uint32_t size, uint64_t remote_addr, uint32_t rkey) {
  RDMAConnection *conn = one_sided_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->RemoteRead(ptr, lkey, size, remote_addr, rkey);
  one_sided_conn_queue_->enqueue(conn);
  return ret;
}

int ConnectionManager::RemoteWrite(void *ptr, uint32_t lkey, uint32_t size, uint64_t remote_addr, uint32_t rkey) {
  RDMAConnection *conn = one_sided_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->RemoteWrite(ptr, lkey, size, remote_addr, rkey);
  one_sided_conn_queue_->enqueue(conn);
  return ret;
}

int ConnectionManager::Free(uint8_t shard,BlockId id) {
  RDMAConnection *conn = rpc_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->Free(shard, id);
  rpc_conn_queue_->enqueue(conn);
  return ret;
}

int ConnectionManager::Lookup(std::string key, uint64_t &addr, uint32_t &rkey, bool &found) {
    RDMAConnection *conn = rpc_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->Lookup(key, addr, rkey, found);
  rpc_conn_queue_->enqueue(conn);
  return ret;
}

}  // namespace kv