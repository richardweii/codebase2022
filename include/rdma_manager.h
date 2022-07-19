#pragma once

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>
#include "config.h"
#include "msg.h"
#include "msg_buf.h"
#include "rdma_conn.h"
#include "util/logging.h"

namespace kv {

struct MemoryAccess {
  uint64_t addr;
  uint32_t rkey;
};

/* The RDMA connection queue */
class ConnQue {
 public:
  ConnQue(size_t size) : size_(size) {
    connections_ = new RDMAConnection *[size];
    in_use_ = new std::atomic<bool>[size] {};
  }

  ~ConnQue() {
    for (size_t i = 0; i < size_; i++) {
      delete connections_[i];
    }
    delete[] connections_;
    delete[] in_use_;
  }

  // init specific connection by create RDMA connection
  bool InitConnection(int idx, ibv_pd *pd, const std::string ip, const std::string port) {
    LOG_ASSERT((size_t)idx < size_, "idx %d", idx);
    connections_[idx] = new RDMAConnection(pd, idx);
    if (connections_[idx]->Init(ip, port)) {
      LOG_ERROR("Init rdma connection %d failed", idx);
      return false;
    }
    return true;
  }

  // init specific connection with given RMDA resource
  bool InitConnection(int idx, ibv_pd *pd, ibv_cq *cq, rdma_cm_id *cm_id) {
    LOG_ASSERT((size_t)idx < size_, "idx %d", idx);
    connections_[idx] = new RDMAConnection(pd, idx);
    if (connections_[idx]->Init(cq, cm_id)) {
      LOG_ERROR("Init rdma connection %d failed", idx);
      return false;
    }
    return true;
  }

  void Enqueue(RDMAConnection *conn) {
    int idx = conn->ConnId();
    LOG_ASSERT((size_t)idx < size_, "idx %d", idx);
    assert(in_use_[idx].load());
    in_use_[idx].store(false);
  };

  RDMAConnection *Dequeue() {
    while (true) {
      for (int i = 0; i < (int)size_; i++) {
        bool tmp = false;
        if (in_use_[i].compare_exchange_weak(tmp, true)) {
          return connections_[i];
        }
      }
      std::this_thread::yield();
    }
    return nullptr;
  }

 private:
  RDMAConnection **connections_ = nullptr;
  std::atomic<bool> *in_use_ = nullptr;
  size_t size_ = 0;
};

class RDMAManager {
 public:
  RDMAManager() {}
  virtual ~RDMAManager() {
    delete msg_buffer_;
    // TODO: release rdma resource
  }

  virtual bool Init(std::string ip, std::string port) = 0;

  virtual void Start() = 0;

  bool Alive() const { return !stop_; }

  virtual void Stop() = 0;

  ibv_mr *RegisterMemory(void *ptr, size_t length);

  ibv_pd *Pd() const { return pd_; }

  int RemoteRead(void *ptr, uint32_t lkey, size_t size, uint64_t remote_addr, uint32_t rkey);

  int RemoteWrite(void *ptr, uint32_t lkey, size_t size, uint64_t remote_addr, uint32_t rkey);

 protected:
  volatile bool stop_;
  MsgBuffer *msg_buffer_ = nullptr;
  ConnQue *rdma_one_side_ = nullptr;
  rdma_event_channel *cm_channel_ = nullptr;
  ibv_context *context_ = nullptr;
  ibv_pd *pd_ = nullptr;

  uint64_t remote_addr_;
  uint32_t remote_rkey_;
  // int signal_counter_ = 0;
};

}  // namespace kv