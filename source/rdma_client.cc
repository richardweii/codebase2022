#include "rdma_client.h"
#include <infiniband/verbs.h>
#include "config.h"
#include "msg.h"
#include "rdma_manager.h"
#include "util/logging.h"

namespace kv {
bool RDMAClient::Init(std::string ip, std::string port) {
  stop_ = false;

  struct ibv_context **ibv_ctxs;
  int nr_devices_;
  ibv_ctxs = rdma_get_devices(&nr_devices_);
  if (!ibv_ctxs) {
    perror("get device list fail");
    return false;
  }

  context_ = ibv_ctxs[0];
  pd_ = ibv_alloc_pd(context_);
  if (!pd_) {
    perror("ibv_alloc_pd fail");
    return false;
  }

  msg_buffer_ = new MsgBuffer(pd_);
  if (!msg_buffer_->Init()) {
    LOG_ERROR("Init MsgBuffer fail.");
    return false;
  }

  rdma_one_side_ = new ConnQue(kOneSideWorkerNum);
  rdma_one_side_->Init(pd_, ip, port, &remote_addr_, &remote_rkey_);
  return true;
}
}  // namespace kv