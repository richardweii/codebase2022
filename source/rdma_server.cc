#include "rdma_server.h"
#include <sched.h>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <thread>
#include "config.h"
#include "msg.h"
#include "msg_buf.h"
#include "pool.h"
#include "rdma_manager.h"
#include "util/logging.h"

namespace kv {
thread_local RDMAConnection *conn_;
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

  rdma_one_side_ = new ConnQue(kOneSideWorkerNum);
  _remote_net_buffer_mr = ibv_reg_mr(pd_, remote_net_buffer_meta, sizeof(NetBuffer::Meta) * kThreadNum, RDMA_MR_FLAG);
  if (_remote_net_buffer_mr == nullptr) {
    LOG_FATAL("_remote_net_buffer_mr register memory failed");
    abort();
  }
  lkey = _remote_net_buffer_mr->lkey;
  for (int i = 0; i < kRPCWorkerNum; i++) {
    workers_.emplace_back(&RDMAServer::worker, this, i);
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

  struct ibv_cq *cq = ibv_create_cq(context_, MAX_CQE, NULL, comp_chan, 0);
  if (!cq) {
    perror("ibv_create_cq fail");
    return -1;
  }

  if (ibv_req_notify_cq(cq, 0)) {
    perror("ibv_req_notify_cq fail");
    return -1;
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
    return -1;
  }

  struct PData rep_pdata;

  rep_pdata.buf_addr = (uintptr_t)msg_buffer_->Data();
  rep_pdata.buf_rkey = msg_buffer_->Rkey();

  struct rdma_conn_param conn_param;
  conn_param.responder_resources = MAX_INITIATOR_DEPTH;
  conn_param.initiator_depth = MAX_INITIATOR_DEPTH;
  conn_param.retry_count = 7;
  conn_param.private_data = &rep_pdata;
  conn_param.private_data_len = sizeof(rep_pdata);

  if (rdma_accept(cm_id, &conn_param)) {
    perror("rdma_accept fail");
    return -1;
  }
  if (worker_num_ < kOneSideWorkerNum) {
    rdma_one_side_->InitConnection(worker_num_, pd_, cq, cm_id);
    worker_num_++;
  }
  return 0;
}

RPCTask *RDMAServer::pollTask(int thread_id) {
  MessageBlock *blocks_ = msg_buffer_->Data();
  while (!stop_) {
    for (size_t i = 0; i < msg_buffer_->Size(); i++) {
      uint8_t notify = PREPARED;
      if (blocks_[i].req_block.notify.compare_exchange_weak(notify, PROCESS)) {
        return new RPCTask(&blocks_[i], this);
      }
    }

    //! 受限于RDMA网卡带宽,此优化实际上没有性能提升,遂关闭
    _start_polling = false;
    if (_start_polling) {
      // 轮询 local端 netbuffer
      bool flag[kThreadNum] = {false};
      RDMAManager::Batch *batchs[kThreadNum];
      uint64_t tails[kThreadNum];
      for (int i = thread_id * kRemoteThreadWorkNum; i < (thread_id + 1) * kRemoteThreadWorkNum; i++) {
        // 轮询local端的第i个线程对应的NetBuffer区域
        // 1. 首先读取buff meta数据到本地
        uint64_t buff_meta_start_off = sizeof(NetBuffer) * i;
        uint64_t buff_data_start_off = buff_meta_start_off + sizeof(NetBuffer::Meta);
        auto batch = this->BeginBatchTL(i);
        batch->RemoteRead(&remote_net_buffer_meta[i], lkey, sizeof(NetBuffer::Meta),
                          _net_buffer_addr + buff_meta_start_off, _net_buffer_rkey);
        batchs[i] = batch;
      }
      for (int i = thread_id * kRemoteThreadWorkNum; i < (thread_id + 1) * kRemoteThreadWorkNum; i++) {
        batchs[i]->FinishBatchTL();
      }

      for (int i = thread_id * kRemoteThreadWorkNum; i < (thread_id + 1) * kRemoteThreadWorkNum; i++) {
        uint64_t buff_meta_start_off = sizeof(NetBuffer) * i;
        uint64_t buff_data_start_off = buff_meta_start_off + sizeof(NetBuffer::Meta);
        // 2. 判断buff_meta是否有任务需要消费
        auto *buff_meta = &remote_net_buffer_meta[i];
        if (!buff_meta->Empty()) {
          // 3. 有任务需要消费, 开始消费任务,将对应的数据读取到remote端
          uint64_t tail = buff_meta->tail;
          uint64_t head = buff_meta->head;
          auto batch = this->BeginBatchTL(i);
          for (; tail != head; tail = ((tail + 1) % kNetBufferPageNum)) {
            batch->RemoteRead((void *)(buff_meta->addrs[tail].remote_addr), buff_meta->addrs[tail].remote_lkey,
                              kPageSize, _net_buffer_addr + buff_data_start_off + kPageSize * tail, _net_buffer_rkey);
          }
          flag[i] = true;
          batchs[i] = batch;
          tails[i] = tail;
        }
      }
      for (int i = thread_id * kRemoteThreadWorkNum; i < (thread_id + 1) * kRemoteThreadWorkNum; i++) {
        if (flag[i]) {
          batchs[i]->FinishBatchTL();
          // 4. 任务完成,更新tail
          auto *buff_meta = &remote_net_buffer_meta[i];
          uint64_t buff_meta_start_off = sizeof(NetBuffer) * i;
          uint64_t buff_data_start_off = buff_meta_start_off + sizeof(NetBuffer::Meta);
          auto batch = this->BeginBatchTL(i);
          buff_meta->tail = tails[i];
          // LOG_INFO("[%d] %d完成, 更新tail为 %ld", thread_id, i, tails[i]);
          batch->RemoteWrite(&remote_net_buffer_meta[i], lkey, sizeof(uint64_t), _net_buffer_addr + buff_meta_start_off,
                             _net_buffer_rkey);
          batchs[i] = batch;
        }
      }
      for (int i = thread_id * kRemoteThreadWorkNum; i < (thread_id + 1) * kRemoteThreadWorkNum; i++) {
        if (flag[i]) {
          batchs[i]->FinishBatchTL();
        }
      }

      int k;
      for (k = thread_id * kRemoteThreadWorkNum; k < (thread_id + 1) * kRemoteThreadWorkNum; k++) {
        if (!flag[k]) break;
      }
      // 当前没有工作,那么sleep一会,避免争用RDMA网卡带宽
      if (k == (thread_id + 1) * kRemoteThreadWorkNum) {
        usleep(100 * 1000);
      }
    }
  }
  return nullptr;
}

// 远端的工作任务
void RDMAServer::worker(int thread_id) {
  while (!stop_) {
    RPCTask *task = pollTask(thread_id);
    if (task == nullptr) {
      break;
    }
    switch (task->RequestType()) {
      case CMD_PING: {
        LOG_DEBUG("Ping message.");
        PingCmd *req = task->GetRequest<PingCmd>();
        this->remote_addr_ = req->addr;
        this->remote_rkey_ = req->rkey;
        PingResponse resp;
        resp.status = RES_OK;
        task->SetResponse(resp);
        break;
      }
      case CMD_STOP: {
        LOG_DEBUG("Stop message.");
        StopResponse resp;
        resp.status = RES_OK;
        task->SetResponse(resp);
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
        task->SetResponse(resp);
        break;
      }
      default:
        handler_(task);
    }

    delete task;
  }
  LOG_INFO("Worker exit.");
}
}  // namespace kv