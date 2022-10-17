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
#include "util/logging.h"
#include "util/nocopy.h"
#include "util/slice.h"

namespace kv {

/* RDMA connection */
class RDMAConnection NOCOPYABLE {
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

  int Init(ibv_cq *cq, rdma_cm_id *cm_id) {
    if (init_) {
      LOG_FATAL("Double init.");
      return -1;
    }
    cq_ = cq;
    cm_id_ = cm_id;
    init_ = true;
    return 0;
  }

  void BeginBatch() {
    is_batch_ = true;
    batch_ = 0;
  }

  int RemoteRead(void *ptr, uint32_t lkey, uint64_t size, uint64_t remote_addr, uint32_t rkey) {
    batch_++;
    return rdma((uint64_t)ptr, lkey, size, remote_addr, rkey, true);
  }

  int RemoteWrite(void *ptr, uint32_t lkey, uint64_t size, uint64_t remote_addr, uint32_t rkey) {
    batch_++;
    return rdma((uint64_t)ptr, lkey, size, remote_addr, rkey, false);
  }

  int FinishBatch() {
    int ret = pollCq(batch_);
    is_batch_ = false;
    return ret;
  }

  int ConnId() const { return conn_id_; }

 private:
  int rdma(uint64_t local_addr, uint32_t lkey, uint64_t size, uint64_t remote_addr, uint32_t rkey, bool read);
  int pollCq(int num);

  struct ibv_comp_channel *comp_chan_;
  struct rdma_event_channel *cm_channel_;
  struct ibv_pd *pd_;
  struct ibv_cq *cq_;
  struct rdma_cm_id *cm_id_;
  bool is_batch_ = false;
  int batch_ = 0;
  bool init_ = false;
  int conn_id_;
};

}  // namespace kv