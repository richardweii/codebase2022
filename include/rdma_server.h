#pragma once
#include <atomic>
#include <cstddef>
#include <cstring>
#include <functional>
#include "msg.h"
#include "rdma_manager.h"

namespace kv {
class RPCTask;
class RDMAServer : public RDMAManager {
 public:
  friend class RPCTask;
  ~RDMAServer() {}

  using Handler = std::function<void(RPCTask *task)>;
  RDMAServer(Handler &&handler) : handler_(std::move(handler)){};

  void Start() override { handleConnection(); }

  void Stop() override {
    for (size_t i = 0; i < workers_.size(); i++) {
      workers_[i].join();
    }
  }

  bool Init(std::string ip, std::string port) override;

 private:
  void handleConnection();

  void worker();

  RPCTask *pollTask();

  int createConnection(rdma_cm_id *cm_id);

  Handler handler_;
  std::vector<std::thread> workers_;

  int worker_num_ = 0;
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
  void SetResponse(const Tp &resp) {
    memcpy(&msg_->resp_block.message, &resp, sizeof(resp));
    msg_->resp_block.notify = DONE;
    server_->RemoteWrite(msg_, server_->msg_buffer_->Lkey(), sizeof(MessageBlock),
                         server_->remote_addr_ + server_->msg_buffer_->MessageAddrOff(msg_), server_->remote_rkey_);
  }

  void FreeAsyncMessage() {
    memset((char *)msg_, 0, sizeof(MessageBlock));
    server_->RemoteWrite(msg_, server_->msg_buffer_->Lkey(), sizeof(MessageBlock),
                         server_->remote_addr_ + server_->msg_buffer_->MessageAddrOff(msg_), server_->remote_rkey_);
    int idx = server_->msg_buffer_->MessageIndex(msg_);
  }

 private:
  MessageBlock *msg_ = nullptr;
  RDMAServer *server_ = nullptr;
};

}  // namespace kv