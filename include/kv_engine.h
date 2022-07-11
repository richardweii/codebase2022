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
#include <atomic>
#include "kv_engine.h"
#include "msg.h"
#include "rdma_conn_manager.h"
#include "rdma_mem_pool.h"
#include "string"
#include "thread"
#include "unordered_map"

#define SHARDING_NUM 64
#define BUCKET_NUM 1048576
static_assert(((SHARDING_NUM & (~SHARDING_NUM + 1)) == SHARDING_NUM),
              "RingBuffer's size must be a positive power of 2");

namespace kv {

typedef struct internal_value_t {
  uint64_t remote_addr;
  uint32_t rkey;
  uint8_t size;
} internal_value_t;

/* One slot stores the key and the meta info of the value which
   describles the remote addr, size, remote-key on remote end. */
class hash_map_slot {
 public:
  char key[16];
  internal_value_t internal_value;
  hash_map_slot *next;
};

class hash_map_t {
 public:
  hash_map_slot *m_bucket[BUCKET_NUM];

  /* Initialize all the pointers to nullptr. */
  hash_map_t() {
    for (int i = 0; i < BUCKET_NUM; ++i) {
      m_bucket[i] = nullptr;
    }
  }

  /* Find the corresponding key. */
  hash_map_slot *find(const std::string &key) {
    int index = std::hash<std::string>()(key) & (BUCKET_NUM - 1);
    if (!m_bucket[index]) {
      return nullptr;
    }
    hash_map_slot *cur = m_bucket[index];
    while (cur) {
      if (memcmp(cur->key, key.c_str(), 16) == 0) {
        return cur;
      }
      cur = cur->next;
    }
    return nullptr;
  }

  /* Insert into the head of the list. */
  void insert(const std::string &key, internal_value_t internal_value,
              hash_map_slot *new_slot) {
    int index = std::hash<std::string>()(key) & (BUCKET_NUM - 1);
    memcpy(new_slot->key, key.c_str(), 16);
    new_slot->internal_value = internal_value;
    if (!m_bucket[index]) {
      m_bucket[index] = new_slot;
    } else {
      /* Insert into the head. */
      hash_map_slot *tmp = m_bucket[index];
      m_bucket[index] = new_slot;
      new_slot->next = tmp;
    }
  }
};

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
  ~LocalEngine(){};

  bool start(const std::string addr, const std::string port) override;
  void stop() override;
  bool alive() override;

  bool write(const std::string key, const std::string value);
  bool read(const std::string key, std::string &value);

 private:
  kv::ConnectionManager *m_rdma_conn_;
  /* NOTE: should use some concurrent data structure, and also should take the
   * extra memory overhead into consideration */
  hash_map_slot hash_slot_array[16 * 12000000];
  hash_map_t m_hash_map[SHARDING_NUM]; /* Hash Map with sharding. */
  std::atomic<int> slot_cnt{0}; /* Used to fetch the slot from hash_slot_array. */
  std::mutex m_mutex_[SHARDING_NUM];
  RDMAMemPool *m_rdma_mem_pool_;
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
  };

  ~RemoteEngine(){};

  bool start(const std::string addr, const std::string port) override;
  void stop() override;
  bool alive() override;

 private:
  void handle_connection();

  int create_connection(struct rdma_cm_id *cm_id);

  struct ibv_mr *rdma_register_memory(void *ptr, uint64_t size);

  int remote_write(WorkerInfo *work_info, uint64_t local_addr, uint32_t lkey,
                   uint32_t length, uint64_t remote_addr, uint32_t rkey);

  int allocate_and_register_memory(uint64_t &addr, uint32_t &rkey,
                                   uint64_t size);

  void worker(WorkerInfo *work_info, uint32_t num);

  struct rdma_event_channel *m_cm_channel_;
  struct rdma_cm_id *m_listen_id_;
  struct ibv_pd *m_pd_;
  struct ibv_context *m_context_;
  bool m_stop_;
  std::thread *m_conn_handler_;
  WorkerInfo **m_worker_info_;
  uint32_t m_worker_num_;
  std::thread **m_worker_threads_;
};

}  // namespace kv