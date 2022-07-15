#include "rdma_manager.h"
#include <infiniband/verbs.h>
#include <netdb.h>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <thread>
#include "config.h"
#include "logging.h"
#include "msg.h"
#include "msg_buf.h"

namespace kv {

ibv_mr *RDMAManager::RegisterMemory(void *ptr, size_t length) {
  auto mr = ibv_reg_mr(pd_, ptr, length, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
  if (mr == nullptr) {
    perror("ibv_reg_mr failed : ");
    LOG_ERROR("Failed to register %zu bytes memory.", length);
    return nullptr;
  }
  return mr;
}

bool RDMAManager::remoteWrite(ibv_qp *qp, uint64_t addr, uint32_t lkey, size_t length, uint64_t remote_addr,
                              uint32_t rkey, bool sync) {
  {
    std::unique_lock<std::mutex> lg(send_mutex_);
    WR wr;
    ibv_sge *sge = wr.sge;
    sge->addr = addr;
    sge->length = length;
    sge->lkey = lkey;

    ibv_send_wr *send_wr = wr.wr;
    send_wr->wr_id = 0;
    send_wr->num_sge = 1;
    send_wr->next = NULL;
    send_wr->opcode = IBV_WR_RDMA_WRITE;
    send_wr->sg_list = sge;
    send_wr->send_flags = IBV_SEND_SIGNALED;
    send_wr->wr.rdma.remote_addr = remote_addr;
    send_wr->wr.rdma.rkey = rkey;
    if (!send_batch_.empty()) {
      send_batch_.back().wr->next = wr.wr;
    }
    send_batch_.push_back(std::move(wr));

    if (sync) {
      postSend(cm_id_->qp);
    }
  }
  send_cv_.notify_one();
  return true;
}

void RDMAManager::rdmaRoutine() {
  while (!stop_) {
    std::unique_lock<std::mutex> lock(send_mutex_);
    send_cv_.wait_until(lock, TIME_NOW + std::chrono::microseconds(RDMA_BATCH_US),
                        [&]() -> bool { return send_batch_.size() >= kRDMABatchNum; });
    if (send_batch_.size() > 0) {
      bool succ = postSend(cm_id_->qp);
      LOG_ASSERT(succ, "post_send failed. batch size %zu", send_batch_.size());
    }
  }
}

bool RDMAManager::postSend(ibv_qp *qp) {
  LOG_ASSERT(send_batch_.size() > 0, "nothign to send.");
  ibv_send_wr *bad_send_wr;
  if (ibv_post_send(qp, send_batch_.front().wr, &bad_send_wr)) {
    perror("ibv_post_send fail");
    LOG_ERROR("ibv_post_send fail.");
    return false;
  }

  // printf("remote write %ld %d\n", remote_addr, rkey);
  bool ret = false;
  auto start = TIME_NOW;
  ibv_wc wc[send_batch_.size()];
  while (true) {
    if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
      LOG_ERROR("rdma_remote_write timeout\n");
      return ret;
    }
    int rc = ibv_poll_cq(cq_, RDMA_MSG_CAP, wc);
    if (rc > 0) {
      assert(rc = send_batch_.size());
      // for (int i = 0; i < rc; i++) {
      if (IBV_WC_SUCCESS == wc[0].status) {
        ret = true;
      } else if (IBV_WC_WR_FLUSH_ERR == wc[0].status) {
        perror("cmd_send IBV_WC_WR_FLUSH_ERR");
        goto end;
      } else if (IBV_WC_RNR_RETRY_EXC_ERR == wc[0].status) {
        perror("cmd_send IBV_WC_RNR_RETRY_EXC_ERR");
        goto end;
      } else {
        perror("cmd_send ibv_poll_cq status error");
        LOG_ERROR("rc %d, no %d, status %s, send batch size %zu", rc, 0, ibv_wc_status_str(wc[0].status), send_batch_.size());
        goto end;
      }
      // }
      break;
    } else if (0 == rc) {
      continue;
    } else {
      perror("ibv_poll_cq fail");
      break;
    }
  }
end:
  send_batch_.clear();
  return ret;
}

}  // namespace kv