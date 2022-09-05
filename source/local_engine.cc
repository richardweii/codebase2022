#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include "assert.h"
#include "atomic"
#include "config.h"
#include "hash_table.h"
#include "kv_engine.h"
#include "pool.h"
#include "rdma_client.h"
#include "stat.h"
#include "util/hash.h"
#include "util/logging.h"
#include "util/slice.h"

namespace kv {

/**
 * @description: start local engine service
 * @param {string} addr    the address string of RemoteEngine to connect
 * @param {string} port   the port of RemoteEngine to connect
 * @return {bool} true for success
 */
bool LocalEngine::start(const std::string addr, const std::string port) {
  constexpr size_t buffer_pool_size = kBufferPoolSize / kPoolShardingNum;
  LOG_INFO("Create %d pool, each pool with %lu MB cache, %lu pages", kPoolShardingNum, buffer_pool_size / 1024 / 1024,
           kPoolSize / kPageSize);
  _client = new RDMAClient();
  if (!_client->Init(addr, port)) return false;
  _client->Start();

  Arena::getInstance().Init(64 * 1024 * 1024);  // 64MB;

  for (int i = 0; i < kPoolShardingNum; i++) {
    _pool[i] = new Pool(i, _client);
    _pool[i]->Init();
  }

  auto watcher = std::thread([]() {
    sleep(1200);
    fflush(stdout);
    abort();
  });
  watcher.detach();

  return true;
}

/**
 * @description: stop local engine service
 * @return {void}
 */
void LocalEngine::stop() {
  _client->Stop();
  delete _client;
  // for (int i = 0; i < kPoolShardingNum; i++) {
  //   delete pool_[i];
  // }
  LOG_INFO(" ========== Performance Statistics ============");
  LOG_INFO(" Total read %ld times, write %ld times", stat::read_times.load(), stat::write_times.load());
  LOG_INFO(" Unique insert %ld  times", stat::insert_num.load());
  LOG_INFO(" Replacement %ld times, Dirty write %ld times ", stat::replacement.load(), stat::dirty_write.load());
  LOG_INFO(" Cache hit %ld times", stat::cache_hit.load());
  LOG_INFO(" Read Miss %ld times", stat::read_miss.load());
  return;
};

/**
 * @description: get engine alive state
 * @return {bool}  true for alive
 */
bool LocalEngine::alive() { return _client->Alive(); }

/**
 * @brief set context of aes, include encode algorithm, key, counter...
 *
 * @return true for success
 * @return false
 */
bool LocalEngine::set_aes() {
  _aes.algo = CTR;

  // key
  Ipp8u key[16] = {0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00};
  _aes.key_len = 16;
  _aes.key = (Ipp8u *)malloc(sizeof(Ipp8u) * _aes.key_len);
  memcpy(_aes.key, key, _aes.key_len);

  // counter
  _aes.blk_size = 16;
  _aes.counter_len = 16;
  Ipp8u ctr[] = {0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 0x09, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00};
  _aes.counter = (Ipp8u *)malloc(sizeof(Ipp8u) * _aes.blk_size);
  memcpy(_aes.counter, ctr, sizeof(ctr));

  // iv <no-use>
  _aes.piv = nullptr;
  _aes.piv_len = 0;

  // counter bit
  _aes.counter_bit = 64;
  return true;
}

crypto_message_t *LocalEngine::get_aes() { return &_aes; }

char *LocalEngine::encrypt(const char *value, size_t len) {
  int ctxSize;               // AES context size
  ippsAESGetSize(&ctxSize);  // evaluating AES context size
  // allocate memory for AES context
  IppsAESSpec *ctx = (IppsAESSpec *)(new Ipp8u[ctxSize]);
  ippsAESInit(_aes.key, _aes.key_len, ctx, ctxSize);
  Ipp8u ctr[_aes.blk_size];
  memcpy(ctr, _aes.counter, _aes.counter_len);
  // allocate memory for ciph
  Ipp8u ciph[len];

  ippsAESEncryptCTR((Ipp8u *)value, ciph, len, ctx, ctr, _aes.counter_bit);
  memcpy((char *)value, ciph, len);

  return (char *)value;
}

char *LocalEngine::decrypt(const char *value, size_t len) {
  crypto_message_t * aes_get = get_aes();
  Ipp8u *ciph = (Ipp8u *)malloc(sizeof(Ipp8u) * len);
  memset(ciph, 0, len);
  memcpy(ciph, value, len);

  int ctxSize;               // AES context size
  ippsAESGetSize(&ctxSize);  // evaluating AES context size
  // allocate memory for AES context
  IppsAESSpec *ctx = (IppsAESSpec *)(new Ipp8u[ctxSize]);
  ippsAESInit(aes_get->key, aes_get->key_len, ctx, ctxSize);
  Ipp8u ctr[aes_get->blk_size];
  memcpy(ctr, aes_get->counter, aes_get->counter_len);

  Ipp8u deciph[len];
  ippsAESDecryptCTR(ciph, deciph, len, ctx, ctr, aes_get->counter_bit);
  memcpy(ciph, deciph, len);
  return (char *)ciph;
}

/**
 * @description: put a key-value pair to engine
 * @param {string} key
 * @param {string} value
 * @return {bool} true for success
 */
bool LocalEngine::write(const std::string &key, const std::string &value, bool use_aes) {
#ifdef STAT
  stat::write_times.fetch_add(1, std::memory_order_relaxed);
  // if (stat::write_times.load(std::memory_order_relaxed) % 1000000 == 0) {
  //   LOG_INFO("write %lu", stat::write_times.load(std::memory_order_relaxed));
  // }
  // if (stat::write_times.load(std::memory_order_relaxed) < 1000) {
  //   LOG_INFO("key %.16s", key.c_str());
  // }
#endif
  uint32_t hash = fuck_hash(key.c_str(), key.size(), kPoolHashSeed);
  int index = Shard(hash);

  if (use_aes) {
    // LOG_INFO("encryption %08lx, %08lx ", *((uint64_t*)(key.data())), *((uint64_t*)(key.data() + 8)));
    char *value_str = encrypt(value.data(), value.length());
    auto succ = _pool[index]->Write(Slice(key), hash, Slice(value_str, value.length()));
    return succ;
  }

  return _pool[index]->Write(Slice(key), hash, Slice(value));
}

/**
 * @description: read value from engine via key
 * @param {string} key
 * @param {string} &value
 * @return {bool}  true for success
 */
bool LocalEngine::read(const std::string &key, std::string &value) {
#ifdef STAT
  stat::read_times.fetch_add(1, std::memory_order_relaxed);
  // if (stat::read_times.load(std::memory_order_relaxed) % 1000000 == 0) {
  //   LOG_INFO("read %lu", stat::read_times.load(std::memory_order_relaxed));
  // }
#endif
  uint32_t hash = fuck_hash(key.c_str(), key.size(), kPoolHashSeed);
  int index = Shard(hash);
  bool succ = _pool[index]->Read(Slice(key), hash, value);
  char *value_str = decrypt(value.c_str(), value.size());
  #ifdef STAT
    if (stat::read_times.load(std::memory_order_relaxed) % 1000000 < 10) {
      LOG_INFO("value: %s, value_str: %s, len: %ld", value.c_str(), value_str, value.size());
    }
  #endif

  return succ;
}

bool LocalEngine::deleteK(const std::string &key) {
  uint32_t hash = fuck_hash(key.c_str(), key.size(), kPoolHashSeed);
  int index = Shard(hash);
  return _pool[index]->Delete(Slice(key), hash);
}

}  // namespace kv