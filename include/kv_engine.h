#pragma once

#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <atomic>
#include <cstdint>
#include "config.h"
#include "msg.h"
#include "pool.h"
#include "rdma_client.h"
#include "rdma_manager.h"
#include "rdma_server.h"
#include "ippcp.h"

namespace kv {

/* Encryption algorithm competitor can choose. */
enum aes_algorithm {
  CTR=0, CBC, CBC_CS1, CBC_CS2, CBC_CS3, CFB, OFB
};

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
  crypto_message_t* get_aes();

  char* encrypt(const char *value, size_t len);
  char* decrypt(const char *value, size_t len);
  
  bool write(const std::string &key, const std::string &value, bool use_aes = false);
  bool read(const std::string &key, std::string &value);
  /** The delete interface */
  bool deleteK(const std::string &key);

 private:
  static uint32_t Shard(uint32_t hash) { return hash % (1 << kPoolShardBits); }

  crypto_message_t aes_;
  Pool *pool_[kPoolShardNum];
  RDMAClient *client_;
};

/* Remote-side engine */
class RemoteEngine : public Engine {
 public:
  ~RemoteEngine(){};

  bool start(const std::string addr, const std::string port) override;
  void stop() override;
  bool alive() override;

 private:
  static uint32_t Shard(uint32_t hash) { return hash % (1 << kPoolShardBits); }
  void handler(RPCTask *task);

  kv::RDMAServer *server_;
  volatile bool stop_;
  RemotePool *pool_[kPoolShardNum];
};

}  // namespace kv