#pragma once
#include "msg.h"
#include "rdma_manager.h"

namespace kv {
class RDMAClient : public RDMAManager {
 public:
  RDMAClient() = default;
  bool Init(std::string ip, std::string port) override;

  void Start() override {
    PingRequest req;
    req.type = MSG_PING;
    req.addr = (uint64_t)msg_buffer_->Data();
    req.rkey = msg_buffer_->Rkey();

    PingResponse resp;
    RPC(&req, resp, true);
    if (resp.status == RES_FAIL) {
      LOG_ERROR("Connect to remote failed.");
    }
  }

  void Stop() override {
    stop_ = true;
    StopRequesst req;
    req.type = MSG_STOP;

    StopResponse resp;
    RPC(&req, resp, true);
    if (resp.status == RES_FAIL) {
      LOG_ERROR("Stop failed.");
    }
  }

  template <typename Req, typename Resp>
  int RPC(Req *req, Resp &resp, bool sync = false);

  int RemoteRead(void *ptr, uint32_t lkey, size_t size, uint64_t remote_addr, uint32_t rkey);

  int RemoteWrite(void *ptr, uint32_t lkey, size_t size, uint64_t remote_addr, uint32_t rkey);

 private:
  ConnQue *one_side_rdma_ = nullptr;
};

template <typename Req, typename Resp>
int RDMAClient::RPC(Req *req, Resp &resp, bool sync) {
  MessageBlock *msg = msg_buffer_->AllocMessage();
  msg->req_block.notify = PREPARED;
  msg->resp_block.notify = PROCESS;
  memcpy(msg->req_block.message, req, sizeof(Req));
  remoteWrite(cm_id_->qp, (uint64_t)msg, msg_buffer_->Lkey(), sizeof(MessageBlock),
              remote_addr_ + msg_buffer_->MessageIndex(msg) * sizeof(MessageBlock), remote_rkey_, sync);

  /* wait for response */
  auto start = TIME_NOW;
  while (msg->resp_block.notify != DONE) {
    if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
      LOG_ERROR("wait for request completion timeout\n");
      return -1;
    }
  }

  ResponseMsg *resp_msg = (ResponseMsg *)msg->resp_block.message;
  memcpy(&resp, resp_msg, sizeof(Resp));
  msg_buffer_->FreeMessage(msg);
  return 0;
}
}  // namespace kv