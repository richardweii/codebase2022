#pragma once

#include "config.h"
#include "msg.h"
#include "rdma_manager.h"
#include "util/logging.h"

namespace kv {
class RDMAClient : public RDMAManager {
 public:
  RDMAClient() = default;
  bool Init(std::string ip, std::string port) override;

  void Start() override {
    PingCmd req;
    req.type = CMD_PING;
    req.addr = (uint64_t)msg_buffer_->Data();
    req.rkey = msg_buffer_->Rkey();
    PingResponse resp;
    RPC(req, resp);
    if (resp.status == RES_FAIL) {
      LOG_ERROR("Connect to remote failed.");
    }
  }

  void Stop() override {
    stop_ = true;
    StopCmd req;
    req.type = CMD_STOP;

    StopResponse resp;
    RPC(req, resp);
    if (resp.status == RES_FAIL) {
      LOG_ERROR("Stop failed.");
    }
  }

  template <typename Req, typename Resp>
  int RPC(const Req &req, Resp &resp);

  template <typename Req>
  int Async(const Req &req);
 private:
};

template <typename Req, typename Resp>
int RDMAClient::RPC(const Req &req, Resp &resp) {
  MessageBlock *msg = msg_buffer_->AllocMessage();
  // LOG_INFO("Alloc msg %d", msg_buffer_->MessageIndex(msg));
  msg->req_block.notify = PREPARED;
  msg->resp_block.notify = PROCESS;
  memcpy(msg->req_block.message, &req, sizeof(Req));
  RemoteWrite(msg, msg_buffer_->Lkey(), sizeof(MessageBlock), remote_addr_ + msg_buffer_->MessageAddrOff(msg),
              remote_rkey_);
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
  // LOG_INFO("Free msg %d", msg_buffer_->MessageIndex(msg));
  return 0;
}

template <typename Req>
int RDMAClient::Async(const Req &req) {
  MessageBlock *msg = msg_buffer_->AllocMessage();
  // LOG_INFO("Alloc msg %d", msg_buffer_->MessageIndex(msg));
  msg->req_block.notify = ASYNC;
  memcpy(msg->req_block.message, &req, sizeof(Req));
  RemoteWrite(msg, msg_buffer_->Lkey(), sizeof(MessageBlock), remote_addr_ + msg_buffer_->MessageAddrOff(msg),
              remote_rkey_);
  // return immediately
  msg_buffer_->FreeAsyncMessage(msg);
  return 0;
}

}  // namespace kv