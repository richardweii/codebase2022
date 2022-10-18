#include "rdma_client.h"
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include "config.h"
#include "msg.h"
#include "rdma_manager.h"
#include "util/logging.h"

namespace kv {
bool RDMAClient::Init(std::string ip, std::string port) {
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

  stop_ = false;

  cm_channel_ = rdma_create_event_channel();
  if (!cm_channel_) {
    perror("rdma_create_event_channel fail");
    return false;
  }
  rdma_cm_id *cm_id;
  if (rdma_create_id(cm_channel_, &cm_id, NULL, RDMA_PS_TCP)) {
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
    if (!rdma_resolve_addr(cm_id, NULL, t->ai_addr, RESOLVE_TIMEOUT_MS)) {
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
    LOG_FATAL("rdma_get_cm_event fail");
    return false;
  }

  if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
    perror("RDMA_CM_EVENT_ADDR_RESOLVED fail");
    LOG_FATAL("RDMA_CM_EVENT_ADDR_RESOLVED fail");
    return false;
  }

  rdma_ack_cm_event(event);

  if (rdma_resolve_route(cm_id, RESOLVE_TIMEOUT_MS)) {
    perror("rdma_resolve_route fail");
    LOG_FATAL("rdma_resolve_route fail");
    return false;
  }

  if (rdma_get_cm_event(cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    LOG_FATAL("rdma_get_cm_event fail");
    return false;
  }

  if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
    printf("aaa: %d\n", event->event);
    perror("RDMA_CM_EVENT_ROUTE_RESOLVED fail");
    LOG_FATAL("RDMA_CM_EVENT_ROUTE_RESOLVED fail");
    return false;
  }

  rdma_ack_cm_event(event);

  msg_buffer_ = new MsgBuffer(pd_);
  if (!msg_buffer_->Init()) {
    LOG_FATAL("Init MsgBuffer fail.");
    return false;
  }

  struct ibv_comp_channel *comp_chan;
  comp_chan = ibv_create_comp_channel(cm_id->verbs);
  if (!comp_chan) {
    perror("ibv_create_comp_channel fail");
    LOG_FATAL("ibv_create_comp_channel fail");
    return false;
  }

  ibv_cq *cq = ibv_create_cq(cm_id->verbs, MAX_CQE, NULL, comp_chan, 0);
  if (!cq) {
    perror("ibv_create_cq fail");
    LOG_FATAL("ibv_create_cq fail");
    return false;
  }

  if (ibv_req_notify_cq(cq, 0)) {
    perror("ibv_req_notify_cq fail");
    LOG_FATAL("ibv_req_notify_cq fail");
    return false;
  }

  struct ibv_qp_init_attr qp_attr = {};
  qp_attr.cap.max_send_wr = MAX_QP_WR;
  qp_attr.cap.max_send_sge = 1;
  qp_attr.cap.max_recv_wr = MAX_QP_WR;
  qp_attr.cap.max_recv_sge = 1;
  qp_attr.cap.max_inline_data = 64;

  qp_attr.send_cq = cq;
  qp_attr.recv_cq = cq;
  qp_attr.qp_type = IBV_QPT_RC;
  if (rdma_create_qp(cm_id, pd_, &qp_attr)) {
    perror("rdma_create_qp fail");
    LOG_FATAL("rdma_create_qp fail");
    return false;
  }

  struct rdma_conn_param conn_param = {};
  conn_param.responder_resources = 16;
  conn_param.initiator_depth = 16;
  conn_param.retry_count = 7;
  if (rdma_connect(cm_id, &conn_param)) {
    perror("rdma_connect fail");
    LOG_FATAL("rdma_connect fail");
    return false;
  }

  if (rdma_get_cm_event(cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    LOG_FATAL("rdma_get_cm_event fail");
    return false;
  }

  if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
    perror("RDMA_CM_EVENT_ESTABLISHED fail");
    LOG_FATAL("RDMA_CM_EVENT_ESTABLISHED fail , got %s", rdma_event_str(event->event));
    return false;
  }

  struct PData server_pdata;
  memcpy(&server_pdata, event->param.conn.private_data, sizeof(server_pdata));

  rdma_ack_cm_event(event);

  remote_addr_ = server_pdata.buf_addr;
  remote_rkey_ = server_pdata.buf_rkey;

  rdma_one_side_ = new ConnQue(kOneSideWorkerNum);
  rdma_one_side_->InitConnection(0, pd_, cq, cm_id);
  for (int i = 1; i < kOneSideWorkerNum; i++) {
    rdma_one_side_->InitConnection(i, pd_, ip, port);
  }
  for (int i = kDirtyFlushConn; i < kOneSideWorkerNum; i++) {
    auto conn = rdma_one_side_->At(i);
    assert(conn != nullptr);
    conn->BeginBatch();
    batchs[i].SetConn(conn);
  }
  return true;
}
}  // namespace kv