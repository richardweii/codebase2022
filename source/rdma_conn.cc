#include "rdma_conn.h"
#include <infiniband/verbs.h>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include "config.h"
#include "msg_buf.h"
#include "util/logging.h"

namespace kv {

int RDMAConnection::Init(const std::string ip, const std::string port) {
  if (init_) {
    LOG_FATAL("Double init.");
    return -1;
  }
  cm_channel_ = rdma_create_event_channel();
  if (!cm_channel_) {
    perror("rdma_create_event_channel fail");
    return -1;
  }

  if (rdma_create_id(cm_channel_, &cm_id_, NULL, RDMA_PS_TCP)) {
    perror("rdma_create_id fail");
    return -1;
  }

  struct addrinfo *res;
  if (getaddrinfo(ip.c_str(), port.c_str(), NULL, &res) < 0) {
    perror("getaddrinfo fail");
    return -1;
  }

  struct addrinfo *t = nullptr;
  for (t = res; t; t = t->ai_next) {
    if (!rdma_resolve_addr(cm_id_, NULL, t->ai_addr, RESOLVE_TIMEOUT_MS)) {
      break;
    }
  }
  freeaddrinfo(res);
  if (!t) {
    perror("getaddrdma_resolve_addrrinfo fail");
    return -1;
  }

  struct rdma_cm_event *event;
  if (rdma_get_cm_event(cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    return -1;
  }

  if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
    perror("RDMA_CM_EVENT_ADDR_RESOLVED fail");
    return -1;
  }

  rdma_ack_cm_event(event);

  if (rdma_resolve_route(cm_id_, RESOLVE_TIMEOUT_MS)) {
    perror("rdma_resolve_route fail");
    return -1;
  }

  if (rdma_get_cm_event(cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    return 1;
  }

  if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
    LOG_FATAL("aaa: %d\n", event->event);
    perror("RDMA_CM_EVENT_ROUTE_RESOLVED fail");
    return -1;
  }

  rdma_ack_cm_event(event);

  comp_chan_ = ibv_create_comp_channel(cm_id_->verbs);
  if (!comp_chan_) {
    perror("ibv_create_comp_channel fail");
    return -1;
  }

  cq_ = ibv_create_cq(cm_id_->verbs, MAX_CQE, NULL, comp_chan_, 0);
  if (!cq_) {
    perror("ibv_create_cq fail");
    return -1;
  }

  if (ibv_req_notify_cq(cq_, 0)) {
    perror("ibv_req_notify_cq fail");
    return -1;
  }

  struct ibv_qp_init_attr qp_attr = {};
  qp_attr.cap.max_send_wr = MAX_QP_WR;
  qp_attr.cap.max_send_sge = 1;
  qp_attr.cap.max_recv_wr = MAX_QP_WR;
  qp_attr.cap.max_recv_sge = 1;
  qp_attr.cap.max_inline_data = 64;

  qp_attr.send_cq = cq_;
  qp_attr.recv_cq = cq_;
  qp_attr.qp_type = IBV_QPT_RC;
  if (rdma_create_qp(cm_id_, pd_, &qp_attr)) {
    perror("rdma_create_qp fail");
    return -1;
  }

  if (rdma_connect(cm_id_, nullptr)) {
    perror("rdma_connect fail");
    LOG_FATAL("rdma_connect fail");
    return false;
  }

  if (rdma_get_cm_event(cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    return -1;
  }

  if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
    perror("RDMA_CM_EVENT_ESTABLISHED fail");
    return -1;
  }
  init_ = true;
  return 0;
}

int RDMAConnection::rdma(uint64_t local_addr, uint32_t lkey, uint64_t size, uint64_t remote_addr, uint32_t rkey,
                         bool read) {
  struct ibv_sge sge;
  sge.addr = (uintptr_t)local_addr;
  sge.length = size;
  sge.lkey = lkey;

  struct ibv_send_wr send_wr = {};
  struct ibv_send_wr *bad_send_wr = nullptr;
  send_wr.wr_id = 0;
  send_wr.num_sge = 1;
  send_wr.next = NULL;
  send_wr.opcode = read ? IBV_WR_RDMA_READ : IBV_WR_RDMA_WRITE;
  send_wr.sg_list = &sge;
  send_wr.send_flags = IBV_SEND_SIGNALED;
  send_wr.wr.rdma.remote_addr = remote_addr;
  send_wr.wr.rdma.rkey = rkey;
  if (ibv_post_send(cm_id_->qp, &send_wr, &bad_send_wr)) {
    perror("ibv_post_send fail");
    abort();
    return -1;
  }

  if (bad_send_wr != nullptr) {
    LOG_FATAL("Bad send wr.");
  }

  if (!is_batch_) {
    return pollCq(1);
  }
  // LOG_FATAL("remote write %ld %d\n", remote_addr, rkey);
  return 0;
}

int RDMAConnection::pollCq(int num) {
  // auto start = TIME_NOW;
  int ret = 0;
  struct ibv_wc wc[num];
  while (num > 0) {
    // if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
    //   LOG_FATAL("rdma_one_side timeout\n");
    //   return -1;
    // }
    int rc = ibv_poll_cq(cq_, num, wc);
    if (rc > 0) {
      for (int i = 0; i < rc; i++) {
        if (IBV_WC_SUCCESS != wc[i].status) {
          LOG_FATAL("poll cq %d/%d failed. Status %d : %s", i, num, wc[i].status, ibv_wc_status_str(wc[i].status));
          perror("cmd_send ibv_poll_cq status error");
          ret = -1;
          break;
        }
      }
      num -= rc;
    } else if (0 == rc) {
      continue;
    } else {
      perror("ibv_poll_cq fail");
      ret = -1;
      break;
    }
  }
  return ret;
}

int RDMAConnection::asyncPollCq(int num) {
  struct ibv_wc wc[num];

  int rc = ibv_poll_cq(cq_, num, wc);
  if (rc > 0) {
    for (int i = 0; i < rc; i++) {
      if (IBV_WC_SUCCESS != wc[i].status) {
        LOG_FATAL("poll cq %d/%d failed. Status %d : %s", i, num, wc[i].status, ibv_wc_status_str(wc[i].status));
        perror("cmd_send ibv_poll_cq status error");
        break;
      }
    }
  }

  batch_ -= rc;
  return rc;
}

}  // namespace kv
