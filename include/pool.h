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
  Pool(uint8_t shard, RDMAClient *client, std::vector<MemoryAccess> *global_rdma_access);
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

  std::vector<MemoryAccess> *_access_table = nullptr;

  BufferPool *_buffer_pool = nullptr;

  RDMAClient *_client = nullptr;

  uint8_t _shard;
  char padding[38 + 64];  // TODO: reset padding
  SpinLatch _latch;
};

class RemotePool NOCOPYABLE {
 public:
  RemotePool(ibv_pd *pd) : _pd(pd) {}
  ~RemotePool() {
    for (auto block : _blocks) {
      if (ibv_dereg_mr(block->_mr)) {
        perror("ibv_dereg_mr failed.");
      }
      delete block;
    }
  }
  // Allocate a datablock for one side write
  MemoryAccess AllocBlock() {
    std::lock_guard<std::mutex> lg(_mutex);
    ValueBlock *block = new ValueBlock();
    auto succ = block->Init(_pd);
    LOG_ASSERT(succ, "Failed to init memblock  %lu.", _blocks.size() + 1);
    _blocks.emplace_back(block);
    _block_num++;
    return {.addr = (uint64_t)block->Data(), .rkey = block->Rkey()};
  }

  int BlockNum() const { return _block_num; }

 private:
  class ValueBlock NOCOPYABLE {
   public:
    friend class RemotePool;
    const char *Data() const { return _data; }
    bool Init(ibv_pd *pd) {
      _mr = ibv_reg_mr(pd, _data, kMaxBlockSize, RDMA_MR_FLAG);
      if (_mr == nullptr) {
        LOG_ERROR("Register memory failed.");
        return false;
      }
      return true;
    }
    uint32_t Rkey() const { return _mr->rkey; }
    uint32_t Lkey() const { return _mr->lkey; }

   private:
    char _data[kMaxBlockSize];
    ibv_mr *_mr = nullptr;
  };
  std::vector<ValueBlock *> _blocks;
  ibv_pd *_pd = nullptr;
  int _block_num = 0;
  std::mutex _mutex;
};

}  // namespace kv