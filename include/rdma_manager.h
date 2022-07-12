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
#include <vector>
#include "config.h"
#include "msg.h"
#include "msg_buf.h"
#include "rdma_conn_manager.h"

namespace kv {

class RDMAManager {
 public:
  RDMAManager();
  virtual ~RDMAManager() {}

  virtual bool Init(std::string ip, std::string port) = 0;

 protected:
  struct WR {
    ibv_sge *sge;
    ibv_send_wr *wr;
    WR() {
      sge = new ibv_sge;
      wr = new ibv_send_wr;
    };
    ~WR() {
      delete sge;
      delete wr;
    }
  };

  // no thread-safe
  bool postSend(ibv_qp *qp);

  bool rdma(ibv_qp *qp, uint64_t addr, uint32_t lkey, size_t length, uint64_t remote_addr, uint32_t rkey,
            ibv_wr_opcode op, bool sync);

  std::vector<WR> send_batch_;
  MsgBuffer *msg_buffer_ = nullptr;
  rdma_event_channel *cm_channel_ = nullptr;
  rdma_cm_id *cm_id_ = nullptr;
  ibv_context *context_ = nullptr;
  ibv_pd *pd_ = nullptr;
  ibv_mr *mr_ = nullptr;
  ibv_cq *cq_ = nullptr;

  uint64_t remote_addr_;
  uint32_t rkey_;

  std::mutex mutex_;
};

class RDMAClient : public RDMAManager {
 public:
  using CallBack = std::function<bool(ResponseMsg &resp)>;

  bool Init(std::string ip, std::string port) override;

  bool RPC(RequestBlock *req, CallBack &&callback);

  bool RemoteRead(uint64_t addr, uint32_t lkey, size_t length, uint64_t remote_addr, uint32_t rkey);

  bool RemoteWrite(uint64_t addr, uint32_t lkey, size_t length, uint64_t remote_addr, uint32_t rkey);

 private:
  ConnQue *one_side_rdma_ = nullptr;
};

class RPCTask;
class RDMAServer : public RDMAManager {
 public:
  friend class RPCTask;
  ~RDMAServer() { delete poller_; }
  RDMAServer(int worker_num = 4);

  void Start() {
    handleConnection();
    poller_->join();
    for (int i = 0; i < workers_.size(); i++) {
      workers_[i].join();
    }
  }

  bool Init(std::string ip, std::string port) override;

 private:
  void handleConnection();

  void poller();

  void worker();

  int createConnection(rdma_cm_id *cm_id);

  volatile bool stop_;

  std::mutex task_mutex_;
  std::condition_variable task_cv_;
  std::queue<RPCTask *> tasks_;

  std::vector<std::thread> workers_;
  std::thread *poller_ = nullptr;
  rdma_cm_id *listen_id_ = nullptr;
};

class RPCTask {
 public:
  RPCTask(MessageBlock *msg, RDMAServer *server) : msg_(msg), server_(server){};

  MsgType RequestType() { return (MsgType)((RequestsMsg *)(&msg_->req_block))->type; }

  template <typename Tp>
  Tp *GetRequest() {
    return static_cast<Tp *>(&msg_->req_block);
  }

  template <typename Tp>
  void SetResponse(const Tp &resp) {
    memcpy(&msg_->resp_block, &resp, sizeof(resp));
    msg_->resp_block.notify = DONE;
    server_->rdma(server_->cm_id_->qp, (uint64_t)msg_, server_->mr_->lkey, sizeof(MessageBlock), server_->remote_addr_,
                  server_->rkey_, IBV_WR_RDMA_WRITE, true);
  }

 private:
  MessageBlock *msg_ = nullptr;
  RDMAServer *server_ = nullptr;
};

}  // namespace kv