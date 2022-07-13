#include "rdma_client.h"
#include <infiniband/verbs.h>
#include "logging.h"
#include "msg.h"
#include "rdma_manager.h"

namespace kv {
bool RDMAClient::Init(std::string ip, std::string port) {
  cm_channel_ = rdma_create_event_channel();
  if (!cm_channel_) {
    perror("rdma_create_event_channel fail");
    return false;
  }

  if (rdma_create_id(cm_channel_, &cm_id_, NULL, RDMA_PS_TCP)) {
    perror("rdma_create_id fail");
    return false;
  }

  struct addrinfo *res;
  if (getaddrinfo(ip.c_str(), port.c_str(), NULL, &res) < 0) {
    perror("getaddrinfo fail");
    return false;
  }

  struct addrinfo *t = nullptr;
  for (t = res; t; t = t->ai_next) {
    if (!rdma_resolve_addr(cm_id_, NULL, t->ai_addr, RESOLVE_TIMEOUT_MS)) {
      break;
    }
  }
  if (!t) {
    perror("getaddrdma_resolve_addrrinfo fail");
    return false;
  }

  freeaddrinfo(res);

  struct rdma_cm_event *event;
  if (rdma_get_cm_event(cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    LOG_ERROR("rdma_get_cm_event fail");
    return false;
  }

  if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
    perror("RDMA_CM_EVENT_ADDR_RESOLVED fail");
    LOG_ERROR("RDMA_CM_EVENT_ADDR_RESOLVED fail");
    return false;
  }

  rdma_ack_cm_event(event);

  if (rdma_resolve_route(cm_id_, RESOLVE_TIMEOUT_MS)) {
    perror("rdma_resolve_route fail");
    LOG_ERROR("rdma_resolve_route fail");
    return false;
  }

  if (rdma_get_cm_event(cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    LOG_ERROR("rdma_get_cm_event fail");
    return false;
  }

  if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
    printf("aaa: %d\n", event->event);
    perror("RDMA_CM_EVENT_ROUTE_RESOLVED fail");
    LOG_ERROR("RDMA_CM_EVENT_ROUTE_RESOLVED fail");
    return false;
  }

  rdma_ack_cm_event(event);

  pd_ = ibv_alloc_pd(cm_id_->verbs);
  if (!pd_) {
    perror("ibv_alloc_pd fail");
    LOG_ERROR("ibv_alloc_pd fail");
    return false;
  }

  msg_buffer_ = new MsgBuffer(pd_);
  if (!msg_buffer_->Init()) {
    LOG_ERROR("Init MsgBuffer fail.");
    return false;
  }


  struct ibv_comp_channel *comp_chan;
  comp_chan = ibv_create_comp_channel(cm_id_->verbs);
  if (!comp_chan) {
    perror("ibv_create_comp_channel fail");
    LOG_ERROR("ibv_create_comp_channel fail");
    return false;
  }

  cq_ = ibv_create_cq(cm_id_->verbs, RDMA_MSG_CAP, NULL, comp_chan, 0);
  if (!cq_) {
    perror("ibv_create_cq fail");
    LOG_ERROR("ibv_create_cq fail");
    return false;
  }

  if (ibv_req_notify_cq(cq_, 0)) {
    perror("ibv_req_notify_cq fail");
    LOG_ERROR("ibv_req_notify_cq fail");
    return false;
  }

  struct ibv_qp_init_attr qp_attr = {};
  qp_attr.cap.max_send_wr = RDMA_MSG_CAP;
  qp_attr.cap.max_send_sge = 1;
  qp_attr.cap.max_recv_wr = 1;
  qp_attr.cap.max_recv_sge = 1;

  qp_attr.send_cq = cq_;
  qp_attr.recv_cq = cq_;
  qp_attr.qp_type = IBV_QPT_RC;
  if (rdma_create_qp(cm_id_, pd_, &qp_attr)) {
    perror("rdma_create_qp fail");
    LOG_ERROR("rdma_create_qp fail");
    return false;
  }

  struct rdma_conn_param conn_param = {};
  conn_param.initiator_depth = 1;
  conn_param.retry_count = 7;
  if (rdma_connect(cm_id_, &conn_param)) {
    perror("rdma_connect fail");
    LOG_ERROR("rdma_connect fail");
    return false;
  }

  if (rdma_get_cm_event(cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    LOG_ERROR("rdma_get_cm_event fail");
    return false;
  }

  if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
    perror("RDMA_CM_EVENT_ESTABLISHED fail");
    LOG_ERROR("RDMA_CM_EVENT_ESTABLISHED fail");
    return false;
  }

  struct PData server_pdata;
  memcpy(&server_pdata, event->param.conn.private_data, sizeof(server_pdata));

  rdma_ack_cm_event(event);

  remote_addr_ = server_pdata.buf_addr;
  remote_rkey_ = server_pdata.buf_rkey;

  one_side_rdma_ = new ConnQue();
  for (uint32_t i = 0; i < kOneSideConnectionNum; i++) {
    RDMAConnection *conn = new RDMAConnection(pd_, i);
    if (conn->Init(ip, port)) {
      LOG_ERROR("Init one side connection %d fail", i);
      return false;
    }
    one_side_rdma_->enqueue(conn);
  }

  return true;
}

int RDMAClient::RemoteRead(void *ptr, uint32_t lkey, size_t size, uint64_t remote_addr, uint32_t rkey) {
  auto conn = one_side_rdma_->dequeue();
  assert(conn != nullptr);
  auto ret = conn->RemoteRead(ptr, lkey, size, remote_addr, rkey);
  assert(ret == 0);
  one_side_rdma_->enqueue(conn);
  return ret;
}

int RDMAClient::RemoteWrite(void *ptr, uint32_t lkey, size_t size, uint64_t remote_addr, uint32_t rkey) {
  auto conn = one_side_rdma_->dequeue();
  assert(conn != nullptr);
  auto ret = conn->RemoteWrite(ptr, lkey, size, remote_addr, rkey);
  assert(ret == 0);
  one_side_rdma_->enqueue(conn);
  return ret;
}

}  // namespace kv