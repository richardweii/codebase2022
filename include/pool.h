#pragma once

#include <infiniband/verbs.h>
#include <atomic>
#include <cassert>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include "buffer_pool.h"
#include "config.h"
#include "hash_table.h"
#include "page_manager.h"
#include "rdma_client.h"
#include "util/nocopy.h"
#include "util/rwlock.h"
#include "util/slice.h"

namespace kv {

class Pool NOCOPYABLE {
 public:
  Pool(uint8_t shard, RDMAClient *client);
  ~Pool();

  void Init();

  bool Read(const Slice &key, uint32_t hash, std::string &val);
  bool Write(const Slice &key, uint32_t hash, const Slice &val);
  bool Delete(const Slice &key, uint32_t hash);

 private:
  void writeNew(const Slice &key, uint32_t hash, const Slice &val);

  PageEntry *mountNewPage(uint8_t slab_class);

  PageEntry *replacement(PageId page_id, uint8_t slab_class);

  void modifyLength(KeySlot *slot, const Slice &val);

  int writeToRemote(PageEntry *entry, RDMAManager::Batch *batch);

  int readFromRemote(PageEntry *entry, PageId page_id, RDMAManager::Batch *batch);

  KeySlot _slots[kKeyNum / kPoolShardingNum];
  int _free_slot_head = -1;  // TODO: lock-free linked list

  HashTable *_hash_index = nullptr;

  PageEntry *_allocing_pages[kSlabSizeMax + 1];
  PageMeta *_allocing_tail[kSlabSizeMax + 1]; 

  MemoryAccess _rdma_access;

  BufferPool *_buffer_pool = nullptr;

  PageManager *_page_manager = nullptr;

  RDMAClient *_client = nullptr;

  uint8_t _shard;
  char padding[38 + 64];  // TODO: reset padding
  SpinLatch _latch;
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
      mr_ = ibv_reg_mr(pd, data_, kPoolSize / kPoolShardingNum, RDMA_MR_FLAG);
      if (mr_ == nullptr) {
        LOG_ERROR("Register memory failed.");
        return false;
      }
      return true;
    }
    uint32_t Rkey() const { return mr_->rkey; }
    uint32_t Lkey() const { return mr_->lkey; }

   private:
    char data_[kPoolSize / kPoolShardingNum];
    ibv_mr *mr_ = nullptr;
  };
  std::vector<ValueBlock *> blocks_;
  ibv_pd *pd_ = nullptr;

  uint8_t shard_;
  std::mutex mutex_;
};

}  // namespace kv