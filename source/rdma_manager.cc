#include "rdma_manager.h"
#include <infiniband/verbs.h>
#include <netdb.h>
#include <mutex>
#include <thread>
#include "config.h"
#include "logging.h"
#include "msg.h"
#include "msg_buf.h"

namespace kv {

bool RDMAManager::rdma(ibv_qp *qp, uint64_t addr, uint32_t lkey, size_t length, uint64_t remote_addr, uint32_t rkey,
                       ibv_wr_opcode op, bool sync) {
  std::lock_guard<std::mutex> lg(mutex_);
  send_batch_.emplace_back();
  ibv_sge *sge = send_batch_.back().sge;
  sge->addr = addr;
  sge->length = length;
  sge->lkey = lkey;

  ibv_send_wr *send_wr = send_batch_.back().wr;
  send_wr->wr_id = 0;
  send_wr->num_sge = 1;
  send_wr->next = NULL;
  send_wr->opcode = op;
  send_wr->sg_list = sge;
  send_wr->send_flags = IBV_SEND_SIGNALED;
  send_wr->wr.rdma.remote_addr = remote_addr;
  send_wr->wr.rdma.rkey = rkey;
  if (sync) {
    return postSend(qp);
  }
  return true;
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
      for (int i = 0; i < rc; i++) {
        if (IBV_WC_SUCCESS == wc[i].status) {
          ret = true;
          break;
        } else if (IBV_WC_WR_FLUSH_ERR == wc[i].status) {
          perror("cmd_send IBV_WC_WR_FLUSH_ERR");
          break;
        } else if (IBV_WC_RNR_RETRY_EXC_ERR == wc[i].status) {
          perror("cmd_send IBV_WC_RNR_RETRY_EXC_ERR");
          break;
        } else {
          perror("cmd_send ibv_poll_cq status error");
          break;
        }
      }
    } else if (0 == rc) {
      continue;
    } else {
      perror("ibv_poll_cq fail");
      break;
    }
  }
  return ret;
}

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
  rkey_ = server_pdata.buf_rkey;

  msg_buffer_ = new MsgBuffer(pd_);
  if (msg_buffer_->Init()) {
    LOG_ERROR("Init MsgBuffer fail.");
    return false;
  }

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

bool RDMAServer::Init(std::string ip, std::string port) {
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

  cm_channel_ = rdma_create_event_channel();
  if (!cm_channel_) {
    perror("rdma_create_event_channel fail");
    return false;
  }

  if (rdma_create_id(cm_channel_, &listen_id_, NULL, RDMA_PS_TCP)) {
    perror("rdma_create_id fail");
    return false;
  }

  struct sockaddr_in sin;
  sin.sin_family = AF_INET;
  sin.sin_port = htons(stoi(port));
  sin.sin_addr.s_addr = INADDR_ANY;

  if (rdma_bind_addr(listen_id_, (struct sockaddr *)&sin)) {
    perror("rdma_bind_addr fail");
    return false;
  }

  if (rdma_listen(listen_id_, 1)) {
    perror("rdma_listen fail");
    return false;
  }

  poller_ = new std::thread(&RDMAServer::poller, this);
  for (int i = 0; i < kWorkerNum; i++) {
    workers_.emplace_back(&RDMAServer::worker, this);
  }

  return true;
}

void RDMAServer::handleConnection() {
  printf("start handle_connection\n");
  struct rdma_cm_event *event;
  while (true) {
    if (stop_) break;
    if (rdma_get_cm_event(cm_channel_, &event)) {
      perror("rdma_get_cm_event fail");
      return;
    }

    if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
      struct rdma_cm_id *cm_id = event->id;
      rdma_ack_cm_event(event);
      createConnection(cm_id);
    } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
      rdma_ack_cm_event(event);
    } else {
      rdma_ack_cm_event(event);
    }
  }
  printf("exit handle_connection\n");
}

int RDMAServer::createConnection(rdma_cm_id *cm_id) {
  if (!pd_) {
    perror("ibv_pibv_alloc_pdoll_cq fail");
    return -1;
  }

  struct ibv_comp_channel *comp_chan = ibv_create_comp_channel(context_);
  if (!comp_chan) {
    perror("ibv_create_comp_channel fail");
    return -1;
  }

  struct ibv_cq *cq = ibv_create_cq(context_, 2, NULL, comp_chan, 0);
  if (!cq) {
    perror("ibv_create_cq fail");
    return -1;
  }

  if (ibv_req_notify_cq(cq, 0)) {
    perror("ibv_req_notify_cq fail");
    return -1;
  }

  struct ibv_qp_init_attr qp_attr = {};
  qp_attr.cap.max_send_wr = 1;
  qp_attr.cap.max_send_sge = 1;
  qp_attr.cap.max_recv_wr = 1;
  qp_attr.cap.max_recv_sge = 1;
  qp_attr.send_cq = cq;
  qp_attr.recv_cq = cq;
  qp_attr.qp_type = IBV_QPT_RC;

  if (rdma_create_qp(cm_id, pd_, &qp_attr)) {
    perror("rdma_create_qp fail");
    return -1;
  }

  struct PData rep_pdata;

  rep_pdata.buf_addr = (uintptr_t)msg_buffer_->Data();
  rep_pdata.buf_rkey = rkey_;

  struct rdma_conn_param conn_param;
  conn_param.responder_resources = 1;
  conn_param.private_data = &rep_pdata;
  conn_param.private_data_len = sizeof(rep_pdata);

  if (rdma_accept(cm_id, &conn_param)) {
    perror("rdma_accept fail");
    return -1;
  }

  if (cm_id_ == nullptr) {
    // rpc connection
    cm_id_ = cm_id;
  }

  return 0;
}

void RDMAServer::poller() {
  MessageBlock *blocks_ = msg_buffer_->Data();
  while (!stop_) {
    {
      std::unique_lock<std::mutex> lock(task_mutex_);
      for (int i = 0; i < msg_buffer_->Size(); i++) {
        if (blocks_[i].req_block.notify == PREPARED) {
          blocks_[i].req_block.notify = PROCESS;
          tasks_.emplace(new RPCTask(&blocks_[i], this));
        }
      }
    }

    task_cv_.notify_all();

    {
      std::unique_lock<std::mutex> lock(task_mutex_);
      task_cv_.wait(lock, [&]() -> bool { return tasks_.size() > kWorkerNum; });
    }
  }
}

void RDMAServer::worker() {
  while (!stop_) {
    RPCTask *task = nullptr;
    {
      std::unique_lock<std::mutex> lock(task_mutex_);
      task_cv_.wait(lock, [&]() -> bool { return tasks_.empty(); });
      task = tasks_.front();
      tasks_.pop();
    }
    // handle the task
    switch (task->RequestType()) {
      case MSG_ALLOC:
        LOG_INFO("Alloc message.");

        break;
      case MSG_FREE:
        LOG_INFO("Free message.");

        break;
      case MSG_PING: {
        LOG_INFO("Ping message.");
        PingRequest *req = task->GetRequest<PingRequest>();
        this->remote_addr_ = req->addr;
        this->rkey_ = req->rkey;
        PingResponse resp;
        resp.status = RES_OK;
        task->SetResponse(resp);
        break;
      }
      case MSG_STOP:
        LOG_INFO("Stop message.");
        break;
      case MSG_LOOKUP:
        LOG_INFO("Lookup message.");

        break;
      case MSG_FETCH:
        LOG_INFO("Fetch message.");

        break;
      default:
        LOG_ERROR("Invalid RPC...");
    }

    delete task;
  }
}

}  // namespace kv