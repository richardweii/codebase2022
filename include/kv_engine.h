#pragma once

#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <atomic>
#include <cstdint>
#include "config.h"
#include "ippcp.h"
#include "msg.h"
#include "pool.h"
#include "rdma_client.h"
#include "rdma_manager.h"
#include "rdma_server.h"

namespace kv {
extern thread_local int cur_thread_id;
extern bool open_compress;
/* Encryption algorithm competitor can choose. */
enum aes_algorithm { CTR = 0, CBC, CBC_CS1, CBC_CS2, CBC_CS3, CFB, OFB };

/* Algorithm relate message. */
typedef struct crypto_message_t {
  aes_algorithm algo;
  Ipp8u *key;
  Ipp32u key_len;
  Ipp8u *counter;
  Ipp32u counter_len;
  Ipp8u *piv;
  Ipp32u piv_len;
  Ipp32u blk_size;
  Ipp32u counter_bit;
} crypto_message_t;

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

  /* Init aes context message. */
  bool set_aes();
  /* Evaluation problem will call this function. */
  crypto_message_t *get_aes();

  char *decrypt(const char *value, size_t len);  // for debug
  bool encrypt(const std::string value, std::string &encrypt_value);

  bool write(const std::string &key, const std::string &value, bool use_aes = false);
  bool read(const std::string &key, std::string &value);
  /** The delete interface */
  bool deleteK(const std::string &key);
  void bind_core() {
    if (UNLIKELY(-1 == cur_thread_id)) {
      cur_thread_id = count_++;
      cur_thread_id %= kThreadNum;
      // cpu_set_t cpuset;
      // CPU_ZERO(&cpuset);
      // CPU_SET(cur_thread_id, &cpuset);
      // auto thread_id = pthread_self();
      // // LOG_INFO("cur_thread_id %d thread_self %ld", cur_thread_id, thread_id);
      // pthread_setaffinity_np(thread_id, sizeof(cpu_set_t), &cpuset);
    }
  }

 private:
  static uint32_t Shard(uint32_t hash) { return hash & kPoolShardingMask; }

  crypto_message_t _aes;
  Pool *_pool;
  RDMAClient *_client;
  MemoryAccess _global_access_table[kMrBlockNum];
  std::atomic<int> count_;
};

/* Remote-side engine */
class RemoteEngine : public Engine {
 public:
  ~RemoteEngine(){};

  bool start(const std::string addr, const std::string port) override;
  void stop() override;
  bool alive() override;
  RemotePool *Pool() const { return _pool; }

 private:
  static uint32_t Shard(uint32_t hash) { return hash % (1 << kPoolShardingBits); }
  void handler(RPCTask *task);

  kv::RDMAServer *_server;
  volatile bool _stop;
  RemotePool *_pool;

  uintptr_t _net_buffer_addr;
  uint32_t _net_buffer_rkey;
  volatile bool _start_polling = false;
};

}  // namespace kv