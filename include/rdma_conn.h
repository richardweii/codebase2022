#pragma once

#include <arpa/inet.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <string>
#include "msg.h"

namespace kv {

#define RESOLVE_TIMEOUT_MS 5000

/* RDMA connection */
class RDMAConnection {
 public:
  int init(const std::string ip, const std::string port);
  int register_remote_memory(uint64_t &addr, uint32_t &rkey, uint64_t size);
  int remote_read(void *ptr, uint64_t size, uint64_t remote_addr,
                  uint32_t rkey);
  int remote_write(void *ptr, uint64_t size, uint64_t remote_addr,
                   uint32_t rkey);

 private:
  struct ibv_mr *rdma_register_memory(void *ptr, uint64_t size);

  int rdma_remote_read(uint64_t local_addr, uint32_t lkey, uint64_t length,
                       uint64_t remote_addr, uint32_t rkey);

  int rdma_remote_write(uint64_t local_addr, uint32_t lkey, uint64_t length,
                        uint64_t remote_addr, uint32_t rkey);

  struct rdma_event_channel *m_cm_channel_;
  struct ibv_pd *m_pd_;
  struct ibv_cq *m_cq_;
  struct rdma_cm_id *m_cm_id_;
  uint64_t m_server_cmd_msg_;
  uint32_t m_server_cmd_rkey_;
  uint32_t m_remote_size_;
  struct CmdMsgBlock *m_cmd_msg_;
  struct CmdMsgRespBlock *m_cmd_resp_;
  struct ibv_mr *m_msg_mr_;
  struct ibv_mr *m_resp_mr_;
  char *m_reg_buf_;
  struct ibv_mr *m_reg_buf_mr_;
};

}  // namespace kv