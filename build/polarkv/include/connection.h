#pragma once

#include <arpa/inet.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include "msg.h"

namespace rdma {

#define RESOLVE_TIMEOUT_MS 5000

/* RDMA connection */
class RDMAConnection {
 public:
  int init(const char *ip, const char *port, ibv_pd* pd);
  void close();
  int register_remote_memory(uint64_t &addr, uint32_t &rkey, uint64_t size);
  int register_remote_memory(uint64_t size, ibv_mr* mr);
  int close_remote_connection();
  int remote_read(uint64_t src, uint32_t size, uint64_t remote_addr,
                  uint32_t rkey);
  int remote_write(uint64_t src, uint32_t size, uint64_t remote_addr,
                   uint32_t rkey);
  ibv_pd* get_pd(){ return m_pd_;}

 private:
  struct ibv_mr *rdma_register_memory(void *ptr, uint32_t size);
  

  int rdma_remote_read(uint64_t local_addr, uint32_t lkey, uint32_t length,
                       uint64_t remote_addr, uint32_t rkey);

  int rdma_remote_write(uint64_t local_addr, uint32_t lkey, uint32_t length,
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

}  // namespace rdma