#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <unordered_map>
#include "assert.h"
#include "atomic"
#include "config.h"
#include "kv_engine.h"
#include "pool.h"
#include "rdma_client.h"
#include "stat.h"
#include "util/hash.h"
#include "util/likely.h"
#include "util/logging.h"
#include "util/rwlock.h"
#include "util/slice.h"

namespace kv {

/**
 * @description: start local engine service
 * @param {string} addr    the address string of RemoteEngine to connect
 * @param {string} port   the port of RemoteEngine to connect
 * @return {bool} true for success
 */
bool LocalEngine::start(const std::string addr, const std::string port) {
  auto time_now = TIME_NOW;
  constexpr size_t buffer_pool_size = kBufferPoolSize / kPoolShardingNum;
  LOG_INFO("Create %d pool, each pool with %lu MB cache, %lu pages", kPoolShardingNum, buffer_pool_size / 1024 / 1024,
           buffer_pool_size / kPageSize);
  _client = new RDMAClient();
  if (!_client->Init(addr, port)) return false;
  _client->Start();

  LOG_INFO("client start");
  Arena::getInstance().Init(64 * 1024 * 1024);  // 64MB;
  global_page_manager = new PageManager(kPoolSize / kPageSize);
  LOG_INFO("global_page_manager created");
  int thread_num = 10;
  std::vector<std::thread> threads;
  // RDMA access global table
  std::vector<MessageBlock *> msgs;
  for (int i = 0; i < kMrBlockNum; i++) {
    AllocRequest req;
    req.size = kMaxBlockSize;
    req.type = MSG_ALLOC;

    AllocResponse resp;
    MessageBlock *msg;
    _client->RPCSend(req, msg);
    msgs.emplace_back(msg);
  }

  int per_thread_num = kMrBlockNum / thread_num;
  for (int t = 0; t < thread_num; t++) {
    threads.emplace_back(
        [&](int tid) {
          for (int i = 0; i < per_thread_num; i++) {
            AllocResponse resp;
            _client->RPCRecv(resp, msgs[i + tid * per_thread_num]);
            if (resp.status != RES_OK) {
              LOG_FATAL("Failed to alloc new block.");
            }

            MemoryAccess access{.addr = resp.addr, .rkey = resp.rkey, .lkey = resp.lkey};
            _global_access_table[i + tid * per_thread_num] = access;
          }
        },
        t);
  }

  _pool = new Pool(0, _client, _global_access_table);
  _pool->Init();

  LOG_INFO("pool init");

  for (auto &th : threads) {
    th.join();
  }

  LOG_INFO("remote Value block allocated");

  auto watcher = std::thread([&]() {
    sleep(60 * 6);
    fflush(stdout);
    abort();
  });
  watcher.detach();

  auto time_end = TIME_NOW;
  auto time_delta = time_end - time_now;
  auto count = std::chrono::duration_cast<std::chrono::microseconds>(time_delta).count();
  LOG_INFO("init time: %lf s", count * 1.0 / 1000 / 1000);

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
  //   delete _pool[i];
  // }
  // delete global_page_manager;
  LOG_INFO(" ========== Performance Statistics ============");
  LOG_INFO(" Total read %ld times, write %ld times", stat::read_times.load(), stat::write_times.load());
  LOG_INFO(" Unique insert %ld  times", stat::insert_num.load());
  LOG_INFO(" modify in place Replacement %ld times, mount new Replacement %ld times ", stat::replacement.load(),
           stat::mount_new_replacement.load());
  LOG_INFO(" dirty write %ld times", stat::dirty_write.load());
  LOG_INFO(" async flush hit %ld times", stat::async_flush.load());
  LOG_INFO(" Cache hit %ld times", stat::cache_hit.load());
  LOG_INFO(" Read Miss %ld times", stat::read_miss.load());
  LOG_INFO(" Delete %ld times", stat::delete_times.load());
  LOG_INFO("hit net buffer %ld", stat::hit_net_buffer.load());
  LOG_INFO("miss net buffer %ld", stat::miss_net_buffer.load());
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
  open_compress = false;
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

crypto_message_t *LocalEngine::get_aes() {
  open_compress = false;
  return &_aes;
}

bool LocalEngine::encrypt(const std::string value, std::string &encrypt_value) {
  open_compress = false;
  assert(value.size() % 16 == 0);
  /*! Size for AES context structure */
  int m_ctxsize = 0;
  /*! Pointer to AES context structure */
  IppsAESSpec *m_pAES = nullptr;
  /*! Error status */
  IppStatus m_status = ippStsNoErr;
  /*! Pointer to encrypted plain text*/
  Ipp8u *m_encrypt_val = nullptr;
  m_encrypt_val = new Ipp8u[value.size()];
  if (nullptr == m_encrypt_val) return false;

  /* 1. Get size needed for AES context structure */
  m_status = ippsAESGetSize(&m_ctxsize);
  if (ippStsNoErr != m_status) return false;
  /* 2. Allocate memory for AES context structure */
  m_pAES = (IppsAESSpec *)(new Ipp8u[m_ctxsize]);
  if (nullptr == m_pAES) return false;
  /* 3. Initialize AES context */
  m_status = ippsAESInit(_aes.key, _aes.key_len, m_pAES, m_ctxsize);
  if (ippStsNoErr != m_status) return false;
  /* 4. counter bits */
  Ipp8u ctr[_aes.blk_size];
  memcpy(ctr, _aes.counter, _aes.counter_len);
  /* 5. Encryption */
  m_status = ippsAESEncryptCTR((Ipp8u *)value.c_str(), m_encrypt_val, value.size(), m_pAES, ctr, _aes.counter_bit);
  if (ippStsNoErr != m_status) return false;
  /* 6. Remove secret and release resources */
  ippsAESInit(0, _aes.key_len, m_pAES, m_ctxsize);

  if (m_pAES) delete[](Ipp8u *) m_pAES;
  m_pAES = nullptr;
  std::string tmp(reinterpret_cast<const char *>(m_encrypt_val), value.size());
  encrypt_value = tmp;

  if (m_encrypt_val) delete[] m_encrypt_val;
  m_encrypt_val = nullptr;
  return true;
}

char *LocalEngine::decrypt(const char *value, size_t len) {
  open_compress = false;
  crypto_message_t *aes_get = get_aes();
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
  bind_core();
#ifdef STAT
  stat::write_times.fetch_add(1, std::memory_order_relaxed);
#endif
  uint32_t hash = Hash(key.c_str(), key.size(), kPoolHashSeed);

  if (UNLIKELY(use_aes)) {
    open_compress = false;
    std::string encrypt_value;
    encrypt(value, encrypt_value);
    assert(value.size() == encrypt_value.size());
    auto succ = _pool->Write(Slice(key), hash, Slice(encrypt_value));
    return succ;
  }

  return _pool->Write(Slice(key), hash, Slice(value));
}

/**
 * @description: read value from engine via key
 * @param {string} key
 * @param {string} &value
 * @return {bool}  true for success
 */
bool LocalEngine::read(const std::string &key, std::string &value) {
  bind_core();
#ifdef STAT
  stat::read_times.fetch_add(1, std::memory_order_relaxed);
#endif
  uint32_t hash = Hash(key.c_str(), key.size(), kPoolHashSeed);

  bool succ = _pool->Read(Slice(key), hash, value);

  return succ;
}

bool LocalEngine::deleteK(const std::string &key) {
  bind_core();
#ifdef STAT
  stat::delete_times.fetch_add(1, std::memory_order_relaxed);
#endif
  uint32_t hash = Hash(key.c_str(), key.size(), kPoolHashSeed);

  return _pool->Delete(Slice(key), hash);
}

// 全局变量，通过config.h extern出去
SpinLock page_locks_[TOTAL_PAGE_NUM];
class PageEntry;
std::shared_ptr<_Result> _do[TOTAL_PAGE_NUM];

thread_local int cur_thread_id = -1;
bool open_compress = true;
bool finished = false;
SpinLock thread_map_lock_;
std::unordered_map<pthread_t, int> thread_map;
}  // namespace kv