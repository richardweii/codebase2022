#pragma once

#include <infiniband/verbs.h>
#include <atomic>
#include <cassert>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include "cache.h"
#include "config.h"
#include "hash_table.h"
#include "rdma_client.h"
#include "rdma_manager.h"
#include "util/nocopy.h"
#include "util/rwlock.h"
#include "util/slice.h"

namespace kv {

class Pool NOCOPYABLE {
 public:
  /**
   * @brief Construct a new Pool object
   *
   * @param cache_size cache data size
   */
  Pool(size_t cache_size, uint8_t shard, RDMAClient *client);
  ~Pool();

  void Init();

  bool Read(const Slice &key, uint32_t hash, std::string &val);
  bool Write(const Slice &key, uint32_t hash, const Slice &val);

 private:
  void writeNew(const Slice &key, const Slice &val);

  int writeToRemote(CacheEntry *entry, RDMAManager::Batch *batch);

  int readFromRemote(CacheEntry *entry, ID id, RDMAManager::Batch *batch);

  CacheEntry *replacement(ID id);

  int allocNewBlock();

  CacheEntry *write_line_ = nullptr;
  uint32_t cache_kv_off_ = 0;

  std::vector<KeyBlock *> keys_;
  HashTable *hash_index_ = nullptr;

  std::unordered_map<BlockId, MemoryAccess> global_addr_table_;
  MemoryAccess cur_block_;
  BlockId cur_block_id_ = 0;
  uint32_t cur_kv_off_ = 0;

  Cache *cache_ = nullptr;

  RDMAClient *client_ = nullptr;

  uint8_t shard_;
  char padding[38 + 64];
  SpinLatch latch_;
};

class RemotePool NOCOPYABLE {
 public:
  RemotePool(ibv_pd *pd, uint8_t shard) : pd_(pd), shard_(shard) {}
  ~RemotePool() {
    for (auto block : blocks_) {
      if (ibv_dereg_mr(block->mr_)) {
        perror("ibv_dereg_mr failed.");
      }
      delete block;
    }
  }
  // Allocate a datablock for one side write
  MemoryAccess AllocBlock() {
    std::lock_guard<std::mutex> lg(mutex_);
    ValueBlock *block = new ValueBlock();
    auto succ = block->Init(pd_);
    LOG_ASSERT(succ, "Shard %d, Failed to init memblock  %lu.", shard_, blocks_.size() + 1);
    blocks_.emplace_back(block);
    return {.addr = (uint64_t)block->Data(), .rkey = block->Rkey()};
  }

 private:
  class ValueBlock NOCOPYABLE {
   public:
    friend class RemotePool;
    const char *Data() const { return data_; }
    bool Init(ibv_pd *pd) {
      mr_ = ibv_reg_mr(pd, data_, kValueBlockSize, RDMA_MR_FLAG);
      if (mr_ == nullptr) {
        LOG_ERROR("Register memory failed.");
        return false;
      }
      return true;
    }
    uint32_t Rkey() const { return mr_->rkey; }
    uint32_t Lkey() const { return mr_->lkey; }

   private:
    char data_[kValueBlockSize];
    ibv_mr *mr_ = nullptr;
  };
  std::vector<ValueBlock *> blocks_;
  ibv_pd *pd_ = nullptr;

  uint8_t shard_;
  std::mutex mutex_;
};

}  // namespace kv