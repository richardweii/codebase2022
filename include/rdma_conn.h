#pragma once

#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <cassert>
#include <cstdint>
#include <string>
#include "config.h"
#include "msg.h"

namespace kv {

/* RDMA connection */
class RDMAConnection  {
 public:
  RDMAConnection(ibv_pd *pd, int id) : pd_(pd), conn_id_(id) {}
  ~RDMAConnection() {
    int ret;
    rdma_destroy_qp(cm_id_);
    ret = ibv_destroy_cq(cq_);
    if (ret != 0) {
      perror("ibv_destory_cq failed.");
    }
    assert(ret == 0);
    ibv_destroy_comp_channel(comp_chan_);
    rdma_destroy_id(cm_id_);
    rdma_destroy_event_channel(cm_channel_);
  };
  int Init(const std::string ip, const std::string port);
  
  int RemoteRead(void *ptr, uint32_t lkey, uint64_t size, uint64_t remote_addr, uint32_t rkey);

  int RemoteWrite(void *ptr, uint32_t lkey, uint64_t size, uint64_t remote_addr, uint32_t rkey);

 private:
  struct ibv_mr *registerMemory(void *ptr, uint64_t size);

  int RDMARead(uint64_t local_addr, uint32_t lkey, uint64_t length, uint64_t remote_addr, uint32_t rkey);

  int RDMAWrite(uint64_t local_addr, uint32_t lkey, uint64_t length, uint64_t remote_addr, uint32_t rkey);

  struct ibv_comp_channel *comp_chan_;
  struct rdma_event_channel *cm_channel_;
  struct ibv_pd *pd_;
  struct ibv_cq *cq_;
  struct rdma_cm_id *cm_id_;

  int conn_id_;
};

}  // namespace kv