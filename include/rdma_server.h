#pragma once
#include <cstddef>
#include "rdma_manager.h"

namespace kv {
class RPCTask;
class RDMAServer : public RDMAManager {
 public:
  friend class RPCTask;
  ~RDMAServer() { delete poller_; }
  RDMAServer() = default;

  void Start() override { handleConnection(); }

  void Stop() override {
    poller_->join();
    for (size_t i = 0; i < workers_.size(); i++) {
      workers_[i].join();
    }
  }

  bool Init(std::string ip, std::string port) override;

 private:
  void handleConnection();

  void poller();

  void worker();

  int createConnection(rdma_cm_id *cm_id);

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
    return reinterpret_cast<Tp *>(msg_->req_block.message);
  }

  template <typename Tp>
  void SetResponse(const Tp &resp, bool sync = false) {
    memcpy(&msg_->resp_block, &resp, sizeof(resp));
    msg_->resp_block.notify = DONE;
    server_->remoteWrite(server_->cm_id_->qp, (uint64_t)msg_, server_->msg_buffer_->Lkey(), sizeof(MessageBlock),
                         server_->remote_addr_, server_->remote_rkey_, sync);
  }

 private:
  MessageBlock *msg_ = nullptr;
  RDMAServer *server_ = nullptr;
};
}  // namespace kv