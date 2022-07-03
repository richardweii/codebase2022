#include "kv_engine.h"

#define MEM_ALIGN_SIZE 4096

namespace kv {

/**
 * @description: start remote engine service
 * @param {string} addr   empty string for RemoteEngine as server
 * @param {string} port   the port the server listened
 * @return {bool} true for success
 */
bool RemoteEngine::start(const std::string addr, const std::string port) {
  m_stop_ = false;

  m_worker_info_ = new WorkerInfo *[MAX_SERVER_WORKER];
  m_worker_threads_ = new std::thread *[MAX_SERVER_WORKER];
  for (uint32_t i = 0; i < MAX_SERVER_WORKER; i++) {
    m_worker_info_[i] = nullptr;
    m_worker_threads_[i] = nullptr;
  }
  m_worker_num_ = 0;

  struct ibv_context **ibv_ctxs;
  int nr_devices_;
  ibv_ctxs = rdma_get_devices(&nr_devices_);
  if (!ibv_ctxs) {
    perror("get device list fail");
    return false;
  }

  m_context_ = ibv_ctxs[0];
  m_pd_ = ibv_alloc_pd(m_context_);
  if (!m_pd_) {
    perror("ibv_alloc_pd fail");
    return false;
  }

  m_cm_channel_ = rdma_create_event_channel();
  if (!m_cm_channel_) {
    perror("rdma_create_event_channel fail");
    return false;
  }

  if (rdma_create_id(m_cm_channel_, &m_listen_id_, NULL, RDMA_PS_TCP)) {
    perror("rdma_create_id fail");
    return false;
  }

  struct sockaddr_in sin;
  sin.sin_family = AF_INET;
  sin.sin_port = htons(stoi(port));
  sin.sin_addr.s_addr = INADDR_ANY;

  if (rdma_bind_addr(m_listen_id_, (struct sockaddr *)&sin)) {
    perror("rdma_bind_addr fail");
    return false;
  }

  if (rdma_listen(m_listen_id_, 1)) {
    perror("rdma_listen fail");
    return false;
  }

  m_conn_handler_ = new std::thread(&RemoteEngine::handle_connection, this);

  m_conn_handler_->join();
  for (uint32_t i = 0; i < MAX_SERVER_WORKER; i++) {
    if (m_worker_threads_[i] != nullptr) {
      m_worker_threads_[i]->join();
    }
  }

  return true;
}

/**
 * @description: get engine alive state
 * @return {bool}  true for alive
 */
bool RemoteEngine::alive() {  // TODO
  return true;
}

/**
 * @description: stop local engine service
 * @return {void}
 */
void RemoteEngine::stop() {
  m_stop_ = true;
  if (m_conn_handler_ != nullptr) {
    m_conn_handler_->join();
    delete m_conn_handler_;
    m_conn_handler_ = nullptr;
  }
  for (uint32_t i = 0; i < MAX_SERVER_WORKER; i++) {
    if (m_worker_threads_[i] != nullptr) {
      m_worker_threads_[i]->join();
      delete m_worker_threads_[i];
      m_worker_threads_[i] = nullptr;
    }
  }
  // TODO: release resources
}

void RemoteEngine::handle_connection() {
  printf("start handle_connection\n");
  struct rdma_cm_event *event;
  while (true) {
    if (m_stop_) break;
    if (rdma_get_cm_event(m_cm_channel_, &event)) {
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
  if (!m_pd_) {
    perror("ibv_pibv_alloc_pdoll_cq fail");
    return -1;
  }

  struct ibv_comp_channel *comp_chan = ibv_create_comp_channel(m_context_);
  if (!comp_chan) {
    perror("ibv_create_comp_channel fail");
    return -1;
  }

  struct ibv_cq *cq = ibv_create_cq(m_context_, 2, NULL, comp_chan, 0);
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

  if (rdma_create_qp(cm_id, m_pd_, &qp_attr)) {
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
  if (!msg_mr) {
    perror("ibv_reg_mr cmd_resp fail");
    return -1;
  }

  rep_pdata.buf_addr = (uintptr_t)cmd_msg;
  rep_pdata.buf_rkey = msg_mr->rkey;
  rep_pdata.size = sizeof(CmdMsgRespBlock);

  int num = m_worker_num_++;
  if (m_worker_num_ <= MAX_SERVER_WORKER) {
    assert(m_worker_info_[num] == nullptr);
    m_worker_info_[num] = new WorkerInfo();
    m_worker_info_[num]->cmd_msg = cmd_msg;
    m_worker_info_[num]->cmd_resp_msg = cmd_resp;
    m_worker_info_[num]->msg_mr = msg_mr;
    m_worker_info_[num]->resp_mr = resp_mr;
    m_worker_info_[num]->cm_id = cm_id;
    m_worker_info_[num]->cq = cq;

    assert(m_worker_threads_[num] == nullptr);
    m_worker_threads_[num] =
        new std::thread(&RemoteEngine::worker, this, m_worker_info_[num], num);
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
      ibv_reg_mr(m_pd_, ptr, size,
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                     IBV_ACCESS_REMOTE_WRITE);
  if (!mr) {
    perror("ibv_reg_mr fail");
    return nullptr;
  }
  return mr;
}

int RemoteEngine::allocate_and_register_memory(uint64_t &addr, uint32_t &rkey,
                                               uint64_t size) {
  /* align mem */
  uint64_t total_size = size + MEM_ALIGN_SIZE;
  uint64_t mem = (uint64_t)malloc(total_size);
  addr = mem;
  if (addr % MEM_ALIGN_SIZE != 0)
    addr = addr + (MEM_ALIGN_SIZE - addr % MEM_ALIGN_SIZE);
  struct ibv_mr *mr = rdma_register_memory((void *)addr, size);
  if (!mr) {
    perror("ibv_reg_mr fail");
    return -1;
  }
  rkey = mr->rkey;
  // printf("allocate and register memory %ld %d\n", addr, rkey);
  // TODO: save this memory info for later delete
  return 0;
}

int RemoteEngine::remote_write(WorkerInfo *work_info, uint64_t local_addr,
                               uint32_t lkey, uint32_t length,
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
  RequestsMsg request;
  while (true) {
    if (m_stop_) break;
    if (cmd_msg->notify == NOTIFY_IDLE) continue;
    cmd_msg->notify = NOTIFY_IDLE;
    RequestsMsg *request = (RequestsMsg *)cmd_msg;
    if (request->type == MSG_REGISTER) {
      /* handle memory register requests */
      RegisterRequest *reg_req = (RegisterRequest *)request;
      // printf("receive a memory register message, size: %ld\n",
      // reg_req->size);
      RegisterResponse *resp_msg = (RegisterResponse *)cmd_resp;
      if (allocate_and_register_memory(resp_msg->addr, resp_msg->rkey,
                                       reg_req->size)) {
        resp_msg->status = RES_FAIL;
      } else {
        resp_msg->status = RES_OK;
      }
      /* write response */
      remote_write(work_info, (uint64_t)cmd_resp, resp_mr->lkey,
                   sizeof(CmdMsgRespBlock), reg_req->resp_addr,
                   reg_req->resp_rkey);
    } else if (request->type == MSG_UNREGISTER) {
      /* handle memory unregister requests */
      UnregisterRequest *unreg_req = (UnregisterRequest *)request;
      printf("receive a memory unregister message, addr: %ld\n",
             unreg_req->addr);
      // TODO: implemente memory unregister
    } else {
      printf("wrong request type\n");
    }
  }
}

}  // namespace kv
