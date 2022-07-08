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
#include <cstdint>
#include <string>
#include "config.h"
#include "msg.h"
#include "util/nocopy.h"

namespace kv {

#define RESOLVE_TIMEOUT_MS 5000

/* RDMA connection */
class RDMAConnection NOCOPYABLE {
 public:
  RDMAConnection(ibv_pd *pd) : pd_(pd) {}
  ~RDMAConnection(){
      // TODO: release resource
  };
  int Init(const std::string ip, const std::string port);

  // Allocate a datablock at the remote
  int AllocDataBlock(uint8_t shard, uint64_t &addr, uint32_t &rkey);

  // lookup a entry at the remote
  int Lookup(std::string key, uint64_t &addr, uint32_t &rkey, bool &found);

  int Free(uint8_t shard, BlockId id);

  int RemoteRead(void *ptr, uint32_t lkey, uint64_t size, uint64_t remote_addr, uint32_t rkey);

  int RemoteWrite(void *ptr, uint32_t lkey, uint64_t size, uint64_t remote_addr, uint32_t rkey);

 private:
  struct ibv_mr *registerMemory(void *ptr, uint64_t size);

  int RDMARead(uint64_t local_addr, uint32_t lkey, uint64_t length, uint64_t remote_addr, uint32_t rkey);

  int RDMAWrite(uint64_t local_addr, uint32_t lkey, uint64_t length, uint64_t remote_addr, uint32_t rkey);

  struct rdma_event_channel *cm_channel_;
  struct ibv_pd *pd_;
  struct ibv_cq *cq_;
  struct rdma_cm_id *cm_id_;

  uint64_t server_cmd_msg_;
  uint32_t server_cmd_rkey_;
  uint32_t remote_size_;

  struct CmdMsgBlock *cmd_msg_;
  struct CmdMsgRespBlock *cmd_resp_;
  struct ibv_mr *msg_mr_;
  struct ibv_mr *resp_mr_;
};

}  // namespace kv