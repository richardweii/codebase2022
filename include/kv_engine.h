#pragma once

#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <cstdint>
#include "config.h"
#include "kv_engine.h"
#include "map"
#include "msg.h"
#include "pool.h"
#include "rdma_conn_manager.h"
#include "string"
#include "thread"

namespace kv {

/* Abstract base engine */
class Engine {
 public:
  virtual ~Engine(){};

  virtual bool start(const std::string addr, const std::string port) = 0;
  virtual void stop() = 0;

  virtual bool alive() = 0;
};

/* Local-side engine */
class LocalEngine : public Engine {
 public:
  struct internal_value_t {
    uint64_t remote_addr;
    uint32_t rkey;
    uint32_t size;
  };

  ~LocalEngine(){};

  bool start(const std::string addr, const std::string port) override;
  void stop() override;
  bool alive() override;

  bool write(const std::string key, const std::string value);
  bool read(const std::string key, std::string &value);

 private:
  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kPoolShardBits); }

  Pool *pool_[kPoolShardNum];
  struct ibv_context **ibv_ctxs_;
  ibv_pd *pd_;
  ConnectionManager *connection_manager_;
  std::shared_ptr<Filter> bloom_filter_;
};

/* Remote-side engine */
class RemoteEngine : public Engine {
 public:
  struct WorkerInfo {
    CmdMsgBlock *cmd_msg;
    CmdMsgRespBlock *cmd_resp_msg;
    struct ibv_mr *msg_mr;
    struct ibv_mr *resp_mr;
    rdma_cm_id *cm_id;
    struct ibv_cq *cq;
    uint64_t remote_addr_;
    uint32_t remote_rkey_;
  };

  ~RemoteEngine(){};

  bool start(const std::string addr, const std::string port) override;
  void stop() override;
  bool alive() override;

 private:
  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kPoolShardBits); }

  void handle_connection();

  int create_connection(struct rdma_cm_id *cm_id);

  struct ibv_mr *rdma_register_memory(void *ptr, uint64_t size);

  int remote_write(WorkerInfo *work_info, uint64_t local_addr, uint32_t lkey, uint32_t length, uint64_t remote_addr,
                   uint32_t rkey);

  void worker(WorkerInfo *work_info, uint32_t num);

  struct rdma_event_channel *cm_channel_;
  struct rdma_cm_id *listen_id_;
  struct ibv_pd *pd_;
  struct ibv_context *context_;
  volatile bool stop_;
  std::thread *conn_handler_;
  WorkerInfo **worker_info_;
  uint32_t worker_num_;
  std::thread **worker_threads_;
  RemotePool *pool_[kPoolShardNum];
  std::shared_ptr<Filter> bloom_filter_;
};

}  // namespace kv