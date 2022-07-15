#pragma once

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
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

namespace kv {

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

class RDMAManager {
 public:
  RDMAManager() { rdma_routine_ = new std::thread(&RDMAManager::rdmaRoutine, this); }
  virtual ~RDMAManager() {
    rdma_routine_->join();
    delete rdma_routine_;
    delete msg_buffer_;
    // TODO: release rdma resource
  }

  virtual bool Init(std::string ip, std::string port) = 0;

  virtual void Start() = 0;

  bool Alive() const { return !stop_; }

  virtual void Stop() = 0;

  ibv_mr *RegisterMemory(void *ptr, size_t length);

  ibv_pd *Pd() const { return pd_; }

 protected:
  void rdmaRoutine();
  struct WR {
    ibv_sge *sge;
    ibv_send_wr *wr;
    WR() {
      sge = new ibv_sge;
      wr = new ibv_send_wr;
    };
    WR(WR &&w) {
      sge = w.sge;
      wr = w.wr;
      w.sge = nullptr;
      w.wr = nullptr;
    }
    ~WR() {
      delete sge;
      delete wr;
    }
  };

  // no thread-safe
  bool postSend(ibv_qp *qp);

  bool remoteWrite(ibv_qp *qp, uint64_t addr, uint32_t lkey, size_t length, uint64_t remote_addr, uint32_t rkey,
                   bool sync);

  volatile bool stop_;

  std::vector<WR> send_batch_;
  std::mutex send_mutex_;
  std::condition_variable send_cv_;

  std::thread *rdma_routine_;

  MsgBuffer *msg_buffer_ = nullptr;
  rdma_event_channel *cm_channel_ = nullptr;
  rdma_cm_id *cm_id_ = nullptr;
  ibv_context *context_ = nullptr;
  ibv_pd *pd_ = nullptr;
  ibv_cq *cq_ = nullptr;

  uint64_t remote_addr_;
  uint32_t remote_rkey_;
  // int signal_counter_ = 0;
};

}  // namespace kv