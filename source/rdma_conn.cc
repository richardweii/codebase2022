#include "rdma_conn.h"
#include "logging.h"

namespace kv {

int RDMAConnection::Init(const std::string ip, const std::string port) {
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
    LOG_ERROR("aaa: %d\n", event->event);
    perror("RDMA_CM_EVENT_ROUTE_RESOLVED fail");
    return -1;
  }

  rdma_ack_cm_event(event);

  comp_chan_ = ibv_create_comp_channel(cm_id_->verbs);
  if (!comp_chan_) {
    perror("ibv_create_comp_channel fail");
    return -1;
  }

  cq_ = ibv_create_cq(cm_id_->verbs, 2, NULL, comp_chan_, 0);
  if (!cq_) {
    perror("ibv_create_cq fail");
    return -1;
  }

  if (ibv_req_notify_cq(cq_, 0)) {
    perror("ibv_req_notify_cq fail");
    return -1;
  }

  struct ibv_qp_init_attr qp_attr = {};
  qp_attr.cap.max_send_wr = 2;
  qp_attr.cap.max_send_sge = 1;
  qp_attr.cap.max_recv_wr = 1;
  qp_attr.cap.max_recv_sge = 1;

  qp_attr.send_cq = cq_;
  qp_attr.recv_cq = cq_;
  qp_attr.qp_type = IBV_QPT_RC;
  if (rdma_create_qp(cm_id_, pd_, &qp_attr)) {
    perror("rdma_create_qp fail");
    return -1;
  }

  if (rdma_connect(cm_id_, nullptr)) {
    perror("rdma_connect fail");
    return -1;
  }

  if (rdma_get_cm_event(cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    return -1;
  }

  if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
    perror("RDMA_CM_EVENT_ESTABLISHED fail");
    return -1;
  }
  return 0;
}

}  // namespace kv
