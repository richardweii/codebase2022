#include "rdma_server.h"
#include <cstddef>
#include <cstring>
#include <thread>
#include "config.h"
#include "util/logging.h"
#include "msg.h"
#include "msg_buf.h"

namespace kv {

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

  msg_buffer_ = new MsgBuffer(pd_);
  auto succ = msg_buffer_->Init();
  if (!succ) {
    LOG_FATAL("init msg buffer failed.");
    return succ;
  }

  poller_ = new std::thread(&RDMAServer::poller, this);
  for (int i = 0; i < kRPCWorkerNum; i++) {
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

  if (cq_ == nullptr) {
    // rpc connection
    cq_ = cq;
  }

  struct ibv_qp_init_attr qp_attr = {};
  qp_attr.cap.max_send_wr = RDMA_MSG_CAP;
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
  rep_pdata.buf_rkey = msg_buffer_->Rkey();

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
      for (size_t i = 0; i < msg_buffer_->Size(); i++) {
        if (blocks_[i].req_block.notify == PREPARED) {
          blocks_[i].req_block.notify = PROCESS;
          tasks_.emplace(new RPCTask(&blocks_[i], this));
        }
      }
    }

    task_cv_.notify_all();

    {
      std::unique_lock<std::mutex> lock(task_mutex_);
      task_cv_.wait(lock, [&]() -> bool { return tasks_.size() <= kRPCWorkerNum; });
    }
  }
}

void RDMAServer::worker() {
  while (!stop_) {
    RPCTask *task = nullptr;
    {
      std::unique_lock<std::mutex> lock(task_mutex_);
      task_cv_.wait(lock, [&]() -> bool { return !tasks_.empty(); });
      task = tasks_.front();
      tasks_.pop();
    }
    task_cv_.notify_one();
    switch (task->RequestType()) {
      case CMD_PING: {
        LOG_DEBUG("Ping message.");
        PingCmd *req = task->GetRequest<PingCmd>();
        this->remote_addr_ = req->addr;
        this->remote_rkey_ = req->rkey;
        PingResponse resp;
        resp.status = RES_OK;
        task->SetResponse(resp, true);
        break;
      }
      case CMD_STOP: {
        LOG_DEBUG("Stop message.");
        StopResponse resp;
        resp.status = RES_OK;
        task->SetResponse(resp, true);
        stop_ = true;
        break;
      }
      case CMD_TEST: {
        LOG_DEBUG("Test message.");
        DummyRequest *req = task->GetRequest<DummyRequest>();
        LOG_DEBUG("Recv %s", req->msg);
        LOG_DEBUG("Msg index %d, rid %d", this->msg_buffer_->MessageIndex((MessageBlock *)req), req->rid);
        DummyResponse resp;
        memset(resp.resp, 0, 16);
        resp.status = RES_OK;
        memcpy(resp.resp, req->msg, 16);
        LOG_DEBUG("Send %s", resp.resp);
        task->SetResponse(resp, req->sync);
        break;
      }
      default:
        handler_(task);
    }

    delete task;
  }
}
}  // namespace kv