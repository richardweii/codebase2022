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
  ~LocalEngine(){};

  bool start(const std::string addr, const std::string port) override;
  void stop() override;
  bool alive() override;

  bool write(const std::string key, const std::string value);
  bool read(const std::string key, std::string &value);

 private:
  static uint32_t Shard(uint32_t hash) { return hash % (1 << kPoolShardBits); }

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