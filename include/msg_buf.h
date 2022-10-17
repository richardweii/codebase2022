#pragma once
#include <infiniband/verbs.h>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <thread>
#include "msg.h"
#include "util/logging.h"

namespace kv {

#define RDMA_MSG_CAP 64

class MsgBuffer {
 public:
  MsgBuffer(ibv_pd *pd) : pd_(pd) {}
  ~MsgBuffer() {
    if (mr_ != nullptr) {
      auto ret = ibv_dereg_mr(mr_);
      if (ret != 0) {
        perror("ibv_dereg_mr failed :");
        LOG_ERROR("Failed to dereg mr");
      }
    }
  }

  bool Init() {
    std::memset((char*)msg_, 0, sizeof(MessageBlock) * RDMA_MSG_CAP);
    mr_ = ibv_reg_mr(pd_, msg_, sizeof(MessageBlock) * RDMA_MSG_CAP,
                     IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    if (mr_ == nullptr) {
      perror("ibv_reg_mr failed : ");
      LOG_FATAL("Failed to registrate mr for MsgBuffer.");
      return false;
    }
    return true;
  }

  MessageBlock *AllocMessage() {
    while (true) {
      for (int i = 0; i < (int)size_; i++) {
        bool tmp = false;
        if (msg_[i].req_block.notify != PREPARED && in_use_[i].compare_exchange_weak(tmp, true)) {
          return &msg_[i];
        }
      }
      std::this_thread::yield();
    }
    return nullptr;
  }

  void FreeMessage(MessageBlock *msg) {
    int off = MessageIndex(msg);
    memset((char *)msg, 0, sizeof(MessageBlock));
    assert(in_use_[off].load());
    in_use_[off].store(false);
  }

  void FreeAsyncMessage(MessageBlock *msg) {
    int off = MessageIndex(msg);
    assert(in_use_[off].load());
    in_use_[off].store(false);
  }

  int MessageIndex(MessageBlock *msg) { return msg - msg_; }

  uint64_t MessageAddrOff(MessageBlock *msg) { return (uint64_t)msg - (uint64_t)msg_; }

  uint32_t Rkey() const { return mr_->rkey; }

  uint32_t Lkey() const { return mr_->lkey; }

  MessageBlock *Data() { return msg_; }

  size_t Size() const { return size_; }

 private:
  size_t size_ = RDMA_MSG_CAP;
  ibv_pd *pd_ = nullptr;
  ibv_mr *mr_ = nullptr;
  MessageBlock msg_[RDMA_MSG_CAP] = {};
  std::atomic<bool> in_use_[RDMA_MSG_CAP] = {};
};

}  // namespace kv