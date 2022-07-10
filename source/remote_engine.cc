#include "config.h"
#include "kv_engine.h"
#include "msg.h"
#include "pool.h"
#include "util/filter.h"
#include "util/hash.h"
#include "util/logging.h"

namespace kv {

/**
 * @description: start remote engine service
 * @param {string} addr   empty string for RemoteEngine as server
 * @param {string} port   the port the server listened
 * @return {bool} true for success
 */
bool RemoteEngine::start(const std::string addr, const std::string port) {
  stop_ = false;

  worker_info_ = new WorkerInfo *[MAX_SERVER_WORKER];
  worker_threads_ = new std::thread *[MAX_SERVER_WORKER];
  for (uint32_t i = 0; i < MAX_SERVER_WORKER; i++) {
    worker_info_[i] = nullptr;
    worker_threads_[i] = nullptr;
  }
  worker_num_ = 0;

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

  for (int i = 0; i < kPoolShardNum; i++) {
    pool_[i] = new RemotePool(pd_, i);
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

  conn_handler_ = new std::thread(&RemoteEngine::handle_connection, this);

  conn_handler_->join();
  for (uint32_t i = 0; i < MAX_SERVER_WORKER; i++) {
    if (worker_threads_[i] != nullptr) {
      worker_threads_[i]->join();
    }
  }

  return true;
}

/**
 * @description: get engine alive state
 * @return {bool}  true for alive
 */
bool RemoteEngine::alive() {  // TODO
  return !stop_;
}

/**
 * @description: stop local engine service
 * @return {void}
 */
void RemoteEngine::stop() {
  stop_ = true;
  if (conn_handler_ != nullptr) {
    conn_handler_->join();
    delete conn_handler_;
    conn_handler_ = nullptr;
  }
  for (uint32_t i = 0; i < MAX_SERVER_WORKER; i++) {
    if (worker_threads_[i] != nullptr) {
      worker_threads_[i]->join();
      delete worker_threads_[i];
      worker_threads_[i] = nullptr;
    }
  }
  // TODO: release resources
}

void RemoteEngine::handle_connection() {
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
      create_connection(cm_id);
    } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
      rdma_ack_cm_event(event);
    } else {
      rdma_ack_cm_event(event);
    }
  }
  printf("exit handle_connection\n");
}

int RemoteEngine::create_connection(struct rdma_cm_id *cm_id) {
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
  CmdMsgBlock *cmd_msg = nullptr;
  CmdMsgRespBlock *cmd_resp = nullptr;
  struct ibv_mr *msg_mr = nullptr;
  struct ibv_mr *resp_mr = nullptr;
  cmd_msg = new CmdMsgBlock();
  memset(cmd_msg, 0, sizeof(CmdMsgBlock));
  msg_mr = rdma_register_memory((void *)cmd_msg, sizeof(CmdMsgBlock));
  if (!msg_mr) {
    perror("ibv_reg_mr cmd_msg fail");
    return -1;
  }

  cmd_resp = new CmdMsgRespBlock();
  memset(cmd_resp, 0, sizeof(CmdMsgRespBlock));
  resp_mr = rdma_register_memory((void *)cmd_resp, sizeof(CmdMsgRespBlock));
  if (!resp_mr) {
    perror("ibv_reg_mr cmd_resp fail");
    return -1;
  }

  rep_pdata.buf_addr = (uintptr_t)cmd_msg;
  rep_pdata.buf_rkey = msg_mr->rkey;
  rep_pdata.size = sizeof(CmdMsgRespBlock);

  int num = worker_num_++;
  if (worker_num_ <= MAX_SERVER_WORKER) {
    assert(worker_info_[num] == nullptr);
    worker_info_[num] = new WorkerInfo();
    worker_info_[num]->cmd_msg = cmd_msg;
    worker_info_[num]->cmd_resp_msg = cmd_resp;
    worker_info_[num]->msg_mr = msg_mr;
    worker_info_[num]->resp_mr = resp_mr;
    worker_info_[num]->cm_id = cm_id;
    worker_info_[num]->cq = cq;

    assert(worker_threads_[num] == nullptr);
    worker_threads_[num] = new std::thread(&RemoteEngine::worker, this, worker_info_[num], num);
  }

  struct rdma_conn_param conn_param;
  conn_param.responder_resources = 1;
  conn_param.private_data = &rep_pdata;
  conn_param.private_data_len = sizeof(rep_pdata);

  // printf("connection created, private data: %ld, addr: %ld, key: %d\n",
  //        *((uint64_t *)rep_pdata.buf_addr), rep_pdata.buf_addr,
  //        rep_pdata.buf_rkey);

  if (rdma_accept(cm_id, &conn_param)) {
    perror("rdma_accept fail");
    return -1;
  }

  return 0;
}

struct ibv_mr *RemoteEngine::rdma_register_memory(void *ptr, uint64_t size) {
  struct ibv_mr *mr =
      ibv_reg_mr(pd_, ptr, size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
  if (!mr) {
    LOG_ERROR("registrate %lu memory failed.", size);
    perror("ibv_reg_mr fail");
    return nullptr;
  }
  return mr;
}

int RemoteEngine::remote_write(WorkerInfo *work_info, uint64_t local_addr, uint32_t lkey, uint32_t length,
                               uint64_t remote_addr, uint32_t rkey) {
  struct ibv_sge sge;
  sge.addr = (uintptr_t)local_addr;
  sge.length = length;
  sge.lkey = lkey;

  struct ibv_send_wr send_wr = {};
  struct ibv_send_wr *bad_send_wr;
  send_wr.wr_id = 0;
  send_wr.num_sge = 1;
  send_wr.next = NULL;
  send_wr.opcode = IBV_WR_RDMA_WRITE;
  send_wr.sg_list = &sge;
  send_wr.send_flags = IBV_SEND_SIGNALED;
  send_wr.wr.rdma.remote_addr = remote_addr;
  send_wr.wr.rdma.rkey = rkey;
  if (ibv_post_send(work_info->cm_id->qp, &send_wr, &bad_send_wr)) {
    perror("ibv_post_send fail");
    return -1;
  }

  // printf("remote write %ld %d\n", remote_addr, rkey);

  auto start = TIME_NOW;
  struct ibv_wc wc;
  int ret = -1;
  while (true) {
    if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
      perror("remote write timeout");
      return -1;
    }
    int rc = ibv_poll_cq(work_info->cq, 1, &wc);
    if (rc > 0) {
      if (IBV_WC_SUCCESS == wc.status) {
        ret = 0;
        break;
      } else if (IBV_WC_WR_FLUSH_ERR == wc.status) {
        perror("cmd_send IBV_WC_WR_FLUSH_ERR");
        break;
      } else if (IBV_WC_RNR_RETRY_EXC_ERR == wc.status) {
        perror("cmd_send IBV_WC_RNR_RETRY_EXC_ERR");
        break;
      } else {
        perror("cmd_send ibv_poll_cq status error");
        break;
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

void RemoteEngine::worker(WorkerInfo *work_info, uint32_t num) {
  printf("start worker %d\n", num);
  CmdMsgBlock *cmd_msg = work_info->cmd_msg;
  CmdMsgRespBlock *cmd_resp = work_info->cmd_resp_msg;
  struct ibv_mr *resp_mr = work_info->resp_mr;
  cmd_resp->notify = NOTIFY_WORK;
  while (true) {
    if (stop_) break;
    if (cmd_msg->notify == NOTIFY_IDLE) continue;
    cmd_msg->notify = NOTIFY_IDLE;
    LOG_DEBUG("Work %d Receive Message", num);
    RequestsMsg *request = (RequestsMsg *)cmd_msg;
    switch (request->type) {
      case MSG_PING: {
        PingMsg *ping = (PingMsg *)request;
        LOG_DEBUG("Client addr %lx rkey %x", ping->resp_addr, ping->resp_rkey);
        work_info->remote_addr_ = ping->resp_addr;
        work_info->remote_rkey_ = ping->resp_rkey;
        break;
      }
      case MSG_STOP: {
        this->stop_ = true;
        return;
      }
      case MSG_ALLOC: {
        AllocRequest *alloc_req = (AllocRequest *)request;
        AllocResponse *alloc_resp = (AllocResponse *)cmd_resp;
        LOG_DEBUG("Alloc msg, shard %d:", alloc_req->shard);
        auto access = pool_[alloc_req->shard]->AllocDataBlock();
        LOG_DEBUG("Alloc successfully, prepare response.");
        alloc_resp->addr = access.data;
        alloc_resp->rkey = access.key;
        alloc_resp->status = RES_OK;
        remote_write(work_info, (uint64_t)cmd_resp, resp_mr->lkey, sizeof(CmdMsgRespBlock), work_info->remote_addr_,
                     work_info->remote_rkey_);
        LOG_DEBUG("Response Alloc msg...");
        break;
      }
      case MSG_LOOKUP: {
        LookupRequest *lookup_req = (LookupRequest *)request;
        LookupResponse *lookup_resp = (LookupResponse *)cmd_resp;
        Key key(new std::string(lookup_req->key, kKeyLength));
        uint32_t hash = Hash(key->c_str(), kKeyLength, kPoolHashSeed);
        int index = Shard(hash);
        LOG_DEBUG("Lookup msg, key %s, shard %d", key->c_str(), index);
        BlockId id = pool_[index]->Lookup(key, NewBloomFilterPolicy());
        if (id == INVALID_BLOCK_ID) {
          lookup_resp->status = RES_FAIL;
          LOG_DEBUG("Failed to find key %s", key->c_str());
        } else {
          auto access = pool_[index]->AccessDataBlock(id);
          lookup_resp->addr = access.data;
          lookup_resp->rkey = access.key;
          lookup_resp->status = RES_OK;
        }
        remote_write(work_info, (uint64_t)cmd_resp, resp_mr->lkey, sizeof(CmdMsgRespBlock), work_info->remote_addr_,
                     work_info->remote_rkey_);
        LOG_DEBUG("Response Lookup msg, blockid %d", id);
        break;
      }
      case MSG_FREE: {
        FreeRequest *free_req = (FreeRequest *)request;
        FreeResponse *free_resp = (FreeResponse *)cmd_resp;
        LOG_DEBUG("Free msg, shard %d block %d:", free_req->shard, free_req->id);
        auto ret = pool_[free_req->shard]->FreeDataBlock(free_req->id);
        LOG_ASSERT(ret, "Failed to free block %d", free_req->id);
        free_resp->status = RES_OK;
        remote_write(work_info, (uint64_t)cmd_resp, resp_mr->lkey, sizeof(CmdMsgRespBlock), work_info->remote_addr_,
                     work_info->remote_rkey_);
        LOG_DEBUG("Response Free msg, blockid %d", free_req->id);
        break;
      }
      default:
        LOG_FATAL("Invalid Message.");
    }
  }
}

}  // namespace kv
