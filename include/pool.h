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
#include "hash_index.h"
#include "page_manager.h"
#include "rdma_client.h"
#include "util/lockfree_queue.h"
#include "util/nocopy.h"
#include "util/rwlock.h"
#include "util/singleflight.h"
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
  bool writeNew(const Slice &key, uint32_t hash, const Slice &val);
  using WriteNewFunc = std::function<bool(const Slice &, uint32_t, const Slice &)>;
  WriteNewFunc _writeNew;

  PageEntry *mountNewPage(uint8_t slab_class, uint32_t hash, RDMAManager::Batch** batch_ret, int tid);

  PageEntry *replacement(PageId page_id, uint8_t slab_class, bool writer = false);
  using ReplacementFunc = std::function<PageEntry *(PageId, uint8_t, bool)>;
  ReplacementFunc _replacement;

  void modifyLength(KeySlot *slot, const Slice &val, uint32_t hash);

  int writeToRemote(PageEntry *entry, RDMAManager::Batch *batch);

  int readFromRemote(PageEntry *entry, PageId page_id, RDMAManager::Batch *batch);

  bool isSmallSlabSize(int slab_size) {
    return slab_size >= 5 && slab_size <= 16;
  }

  void allocingListWLock(uint32_t al_index, int slab_size) {
    if (isSmallSlabSize(slab_size)) {
      _small_allocing_list_latch[al_index][slab_size].WLock();
    } else {
      _big_allocing_list_latch[slab_size].WLock();
    }
  }

  void allocingListWUnlock(uint32_t al_index, int slab_size) {
    if (isSmallSlabSize(slab_size)) {
      _small_allocing_list_latch[al_index][slab_size].WUnlock();
    } else {
      _big_allocing_list_latch[slab_size].WUnlock();
    }
  }

  HashTable *_hash_index = nullptr;

  SingleFlight<PageId, PageEntry *> _replacement_sgfl;
  SingleFlight<std::string, bool> _write_new_sgfl;

  // small page只存储 slab size为 5 ~ 16 的
  PageEntry *_small_allocing_pages[kAllocingListShard][kSlabSizeMax + 1];
  PageMeta *_small_allocing_tail[kAllocingListShard][kSlabSizeMax + 1];
  // big page存储 slab size 为16 ~ 64 的
  PageEntry *_big_allocing_pages[kSlabSizeMax + 1];
  PageMeta *_big_allocing_tail[kSlabSizeMax + 1];
  // allocing list latch
  SpinLatch _small_allocing_list_latch[kAllocingListShard][kSlabSizeMax + 1];
  SpinLatch _big_allocing_list_latch[kSlabSizeMax + 1];

  std::vector<MemoryAccess> *_access_table = nullptr;

  BufferPool *_buffer_pool = nullptr;

  RDMAClient *_client = nullptr;

  uint8_t _shard;

  SpinLock _lock;
  SpinLatch _latch;
};

class RemotePool NOCOPYABLE {
 public:
  RemotePool(ibv_pd *pd) : _pd(pd) { _blocks = new ValueBlock[kMrBlockNum]; }
  ~RemotePool() {}
  // Allocate a datablock for one side write
  MemoryAccess AllocBlock() {
    int cur = _block_cnt.fetch_add(1);
    ValueBlock *block = &_blocks[cur];
    auto succ = block->Init(_pd);
    LOG_ASSERT(succ, "Failed to init memblock  %d.", cur);
    LOG_INFO("Alloc block %d successfully, prepare response.", cur);
    return {.addr = (uint64_t)block->Data(), .rkey = block->Rkey()};
  }

  int BlockNum() const { return _block_cnt; }

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
  ValueBlock *_blocks = nullptr;
  ibv_pd *_pd = nullptr;
  std::atomic<int> _block_cnt{0};
};

}  // namespace kv