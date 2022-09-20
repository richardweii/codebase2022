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
  using WriteNewFunc = std::function<bool(const Slice &key, uint32_t hash, const Slice &val)>;
  WriteNewFunc _writeNew;

  PageEntry *mountNewPage(uint8_t slab_class);

  PageEntry *replacement(PageId page_id, uint8_t slab_class);
  using ReplacementFunc = std::function<PageEntry *(PageId page_id, uint8_t slab_class)>;
  ReplacementFunc _replacement;

  void modifyLength(KeySlot *slot, const Slice &val);

  int writeToRemote(PageEntry *entry, RDMAManager::Batch *batch);

  int readFromRemote(PageEntry *entry, PageId page_id, RDMAManager::Batch *batch);

  HashTable *_hash_index = nullptr;

  SingleFlight<PageId, PageEntry *> _replacement_sgfl;
  SingleFlight<std::string, bool> _write_new_sgfl;

  PageEntry *_allocing_pages[kSlabSizeMax + 1];
  PageMeta *_allocing_tail[kSlabSizeMax + 1];

  std::vector<MemoryAccess> *_access_table = nullptr;

  BufferPool *_buffer_pool = nullptr;

  RDMAClient *_client = nullptr;

  uint8_t _shard;

  SpinLatch _allocing_list_latch[kSlabSizeMax + 1];
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