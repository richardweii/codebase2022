#pragma once
#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <functional>
#include "config.h"
#include "msg.h"
#include "pool.h"
#include "rdma_conn.h"
#include "rdma_manager.h"
#include "util/logging.h"

namespace kv {
extern thread_local RDMAConnection *conn_;

class RPCTask;
class RDMAServer : public RDMAManager {
 public:
  friend class RPCTask;
  ~RDMAServer() {}

  using Handler = std::function<void(RPCTask *task)>;
  RDMAServer(Handler &&handler) : handler_(std::move(handler)){};

  void Start() override {
    handleConnection();
  }

  void Stop() override {
    for (size_t i = 0; i < workers_.size(); i++) {
      workers_[i].join();
    }
  }

  bool Init(std::string ip, std::string port) override;
  uintptr_t _net_buffer_addr;
  uint32_t _net_buffer_rkey;
  volatile bool _start_polling = false;
  RemotePool* pool_;

 private:
  void handleConnection();

  void worker(int thread_id);

  RPCTask *pollTask(int thread_id);

  int createConnection(rdma_cm_id *cm_id);

  Handler handler_;
  std::vector<std::thread> workers_;

  int worker_num_ = 0;
  rdma_cm_id *listen_id_ = nullptr;
  ibv_mr *_remote_net_buffer_mr;
  uint32_t lkey;
  NetBuffer remote_net_buffer[kThreadNum];
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