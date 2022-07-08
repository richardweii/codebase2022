#include "rdma_conn.h"
#include <cstdint>
#include <cstring>
#include "config.h"
#include "msg.h"
#include "util/logging.h"

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

  struct ibv_comp_channel *comp_chan;
  comp_chan = ibv_create_comp_channel(cm_id_->verbs);
  if (!comp_chan) {
    perror("ibv_create_comp_channel fail");
    return -1;
  }

  cq_ = ibv_create_cq(cm_id_->verbs, 2, NULL, comp_chan, 0);
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

  cmd_msg_ = new CmdMsgBlock();
  memset(cmd_msg_, 0, sizeof(CmdMsgBlock));
  msg_mr_ = registerMemory((void *)cmd_msg_, sizeof(CmdMsgBlock));
  if (!msg_mr_) {
    perror("ibv_reg_mr m_msg_mr_ fail");
    return -1;
  }

  cmd_resp_ = new CmdMsgRespBlock();
  memset(cmd_resp_, 0, sizeof(CmdMsgRespBlock));
  resp_mr_ = registerMemory((void *)cmd_resp_, sizeof(CmdMsgRespBlock));
  if (!resp_mr_) {
    perror("ibv_reg_mr m_resp_mr_ fail");
    return -1;
  }

  struct PData rep_pdata;

  rep_pdata.buf_addr = (uint64_t)cmd_resp_;
  rep_pdata.buf_rkey = resp_mr_->rkey;
  LOG_DEBUG("local addr %lx rkey %x", rep_pdata.buf_addr, rep_pdata.buf_rkey);

  struct rdma_conn_param conn_param = {};
  conn_param.private_data = &rep_pdata;
  conn_param.private_data_len = sizeof(rep_pdata);
  conn_param.initiator_depth = 1;
  conn_param.retry_count = 7;
  if (rdma_connect(cm_id_, &conn_param)) {
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

  struct PData server_pdata;
  memcpy(&server_pdata, event->param.conn.private_data, sizeof(server_pdata));

  rdma_ack_cm_event(event);

  server_cmd_msg_ = server_pdata.buf_addr;
  server_cmd_rkey_ = server_pdata.buf_rkey;
  assert(server_pdata.size == sizeof(CmdMsgBlock));
  return 0;
}

struct ibv_mr *RDMAConnection::registerMemory(void *ptr, uint64_t size) {
  struct ibv_mr *mr =
      ibv_reg_mr(pd_, ptr, size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
  if (!mr) {
    perror("ibv_reg_mr fail");
    return nullptr;
  }
  return mr;
}

int RDMAConnection::RDMARead(uint64_t local_addr, uint32_t lkey, uint64_t length, uint64_t remote_addr, uint32_t rkey) {
  struct ibv_sge sge;
  sge.addr = (uintptr_t)local_addr;
  sge.length = length;
  sge.lkey = lkey;

  struct ibv_send_wr send_wr = {};
  struct ibv_send_wr *bad_send_wr;
  send_wr.wr_id = 0;
  send_wr.num_sge = 1;
  send_wr.next = NULL;
  send_wr.opcode = IBV_WR_RDMA_READ;
  send_wr.sg_list = &sge;
  send_wr.send_flags = IBV_SEND_SIGNALED;
  send_wr.wr.rdma.remote_addr = remote_addr;
  send_wr.wr.rdma.rkey = rkey;
  if (ibv_post_send(cm_id_->qp, &send_wr, &bad_send_wr)) {
    perror("ibv_post_send fail");
    return -1;
  }

  // LOG_ERROR("remote read %ld %d\n", remote_addr, rkey);
  auto start = TIME_NOW;
  int ret = -1;
  struct ibv_wc wc;
  while (true) {
    if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
      LOG_ERROR("rdma_remote_read timeout\n");
      return -1;
    }
    int rc = ibv_poll_cq(cq_, 1, &wc);
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

int RDMAConnection::RDMAWrite(uint64_t local_addr, uint32_t lkey, uint64_t length, uint64_t remote_addr,
                              uint32_t rkey) {
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
  if (ibv_post_send(cm_id_->qp, &send_wr, &bad_send_wr)) {
    perror("ibv_post_send fail");
    return -1;
  }

  // LOG_ERROR("remote write %ld %d\n", remote_addr, rkey);

  auto start = TIME_NOW;
  int ret = -1;
  struct ibv_wc wc;
  while (true) {
    if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
      LOG_ERROR("rdma_remote_write timeout\n");
      return -1;
    }
    int rc = ibv_poll_cq(cq_, 1, &wc);
    if (rc > 0) {
      if (IBV_WC_SUCCESS == wc.status) {
        // Break out as operation completed successfully
        // LOG_ERROR("Break out as operation completed successfully\n");
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

int RDMAConnection::RemoteRead(void *ptr, uint32_t lkey, uint64_t size, uint64_t remote_addr, uint32_t rkey) {
  int ret = RDMARead((uint64_t)ptr, lkey, size, remote_addr, rkey);
  if (ret) {
    return -1;
  }
  return ret;
}

int RDMAConnection::RemoteWrite(void *ptr, uint32_t lkey, uint64_t size, uint64_t remote_addr, uint32_t rkey) {
  return RDMAWrite((uint64_t)ptr, lkey, size, remote_addr, rkey);
}

int RDMAConnection::AllocDataBlock(uint8_t shard, uint64_t &addr, uint32_t &rkey) {
  LOG_DEBUG("Connection %d AllocDataBlock, shard %d", conn_id_, shard);
  memset(cmd_msg_, 0, sizeof(CmdMsgBlock));
  memset(cmd_resp_, 0, sizeof(CmdMsgRespBlock));
  cmd_resp_->notify = NOTIFY_IDLE;
  AllocRequest *request = (AllocRequest *)cmd_msg_;
  request->type = MSG_ALLOC;
  request->shard = shard;
  cmd_msg_->notify = NOTIFY_WORK;

  /* send a request to sever */
  int ret = RDMAWrite((uint64_t)cmd_msg_, msg_mr_->lkey, sizeof(CmdMsgBlock), server_cmd_msg_, server_cmd_rkey_);
  if (ret) {
    LOG_ERROR("fail to send requests\n");
    return ret;
  }

  /* wait for response */
  auto start = TIME_NOW;
  while (cmd_resp_->notify == NOTIFY_IDLE) {
    if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
      LOG_ERROR("wait for request completion timeout\n");
      return -1;
    }
  }
  AllocResponse *resp_msg = (AllocResponse *)cmd_resp_;
  if (resp_msg->status != RES_OK) {
    LOG_ERROR("register remote memory fail\n");
    return -1;
  }
  addr = resp_msg->addr;
  rkey = resp_msg->rkey;
  // LOG_ERROR("receive response: addr: %ld, key: %d\n", resp_msg->addr,
  //  resp_msg->rkey);
  return 0;
}

int RDMAConnection::Ping() {
  memset(cmd_msg_, 0, sizeof(CmdMsgBlock));
  memset(cmd_resp_, 0, sizeof(CmdMsgRespBlock));
  cmd_resp_->notify = NOTIFY_IDLE;
  PingMsg *request = (PingMsg *)cmd_msg_;
  request->type = MSG_PING;
  request->resp_addr = (uint64_t)cmd_resp_;
  request->resp_rkey = resp_mr_->rkey;
  cmd_msg_->notify = NOTIFY_WORK;

  /* send a request to sever */
  int ret = RDMAWrite((uint64_t)cmd_msg_, msg_mr_->lkey, sizeof(CmdMsgBlock), server_cmd_msg_, server_cmd_rkey_);
  if (ret) {
    LOG_ERROR("fail to send requests\n");
    return ret;
  }
  // no need response
  return 0;
}

int RDMAConnection::Lookup(std::string key, uint64_t &addr, uint32_t &rkey, bool &found) {
  LOG_DEBUG("Connection %d Lookup, key %s", conn_id_, key.c_str());
  memset(cmd_msg_, 0, sizeof(CmdMsgBlock));
  memset(cmd_resp_, 0, sizeof(CmdMsgRespBlock));
  cmd_resp_->notify = NOTIFY_IDLE;
  LookupRequest *request = (LookupRequest *)cmd_msg_;
  request->type = MSG_LOOKUP;
  memcpy(request->key, key.c_str(), key.size());
  cmd_msg_->notify = NOTIFY_WORK;

  /* send a request to sever */
  int ret = RDMAWrite((uint64_t)cmd_msg_, msg_mr_->lkey, sizeof(CmdMsgBlock), server_cmd_msg_, server_cmd_rkey_);
  if (ret) {
    LOG_ERROR("fail to send requests\n");
    return ret;
  }

  /* wait for response */
  auto start = TIME_NOW;
  while (cmd_resp_->notify == NOTIFY_IDLE) {
    if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
      LOG_ERROR("wait for request completion timeout\n");
      return -1;
    }
  }
  LookupResponse *resp_msg = (LookupResponse *)cmd_resp_;
  if (resp_msg->status != RES_OK) {
    found = false;
    return 0;
  }
  addr = resp_msg->addr;
  rkey = resp_msg->rkey;
  // LOG_ERROR("receive response: addr: %ld, key: %d\n", resp_msg->addr,
  //  resp_msg->rkey);
  return 0;
}

int RDMAConnection::Free(uint8_t shard, BlockId id) {
  memset(cmd_msg_, 0, sizeof(CmdMsgBlock));
  memset(cmd_resp_, 0, sizeof(CmdMsgRespBlock));
  cmd_resp_->notify = NOTIFY_IDLE;
  FreeRequest *request = (FreeRequest *)cmd_msg_;
  request->type = MSG_FREE;
  request->id = id;
  request->shard = shard;
  cmd_msg_->notify = NOTIFY_WORK;

  /* send a request to sever */
  int ret = RDMAWrite((uint64_t)cmd_msg_, msg_mr_->lkey, sizeof(CmdMsgBlock), server_cmd_msg_, server_cmd_rkey_);
  if (ret) {
    LOG_ERROR("fail to send requests\n");
    return ret;
  }
  // no need response
  return 0;
}

}  // namespace kv
