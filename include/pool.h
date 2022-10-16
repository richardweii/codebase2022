#pragma once

#include <infiniband/verbs.h>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include "buffer_pool.h"
#include "config.h"
#include "hash_index.h"
#include "page_manager.h"
#include "rdma_client.h"
#include "util/logging.h"
#include "util/nocopy.h"
#include "util/rwlock.h"
#include "util/singleflight.h"
#include "util/slice.h"

namespace kv {

// writeToRemote的时候,如果NetBuffer有空间,那么可以直接将要置换到远端的Page存到NetBuffer当中,然后直接返回即可,不用自己写到远端,
// 远端机器轮询这块区域,主动读到对应的page的位置
class NetBuffer NOCOPYABLE {
 public:
  struct Meta {
    uint64_t ready;
    uint64_t remote_addr;
    std::atomic<uint64_t> head;
    std::atomic<uint64_t> tail;
  };
  struct Entry {
    PageData data;
  };
  Meta buff_meta[kNetBufferPageNum];
  Entry buff_data[kNetBufferPageNum];
};

class Pool NOCOPYABLE {
 public:
  Pool(uint8_t shard, RDMAClient *client, MemoryAccess *global_rdma_access);
  ~Pool();

  void Init();

  bool Read(const Slice &key, uint32_t hash, std::string &val);
  bool Write(const Slice &key, uint32_t hash, const Slice &val);
  bool Delete(const Slice &key, uint32_t hash);

 private:
  bool writeNew(const Slice &key, uint32_t hash, const Slice &val);
  using WriteNewFunc = std::function<bool(const Slice &, uint32_t, const Slice &)>;
  WriteNewFunc _writeNew;

  PageEntry *mountNewPage(uint8_t slab_class, uint32_t hash, RDMAManager::Batch **batch_ret, int tid);

  PageEntry *replacement(PageId page_id, uint8_t slab_class, bool writer = false);
  using ReplacementFunc = std::function<PageEntry *(PageId, uint8_t, bool)>;
  ReplacementFunc _replacement;

  void modifyLength(KeySlot *slot, const Slice &val, uint32_t hash);

  int writeToRemote(PageEntry *entry, RDMAManager::Batch *batch);

  int readFromRemote(PageEntry *entry, PageId page_id, RDMAManager::Batch *batch);

  void allocingListWLock(uint32_t al_index, int slab_size) { _allocing_list_latch[al_index][slab_size].WLock(); }

  void allocingListWUnlock(uint32_t al_index, int slab_size) { _allocing_list_latch[al_index][slab_size].WUnlock(); }

  HashTable *_hash_index = nullptr;

  SingleFlight<PageId, PageEntry *> _replacement_sgfl;
  SingleFlight<std::string, bool> _write_new_sgfl;

  PageEntry *_allocing_pages[kAllocingListShard][kSlabSizeMax + 1];
  PageMeta *_allocing_tail[kAllocingListShard][kSlabSizeMax + 1];
  // allocing list latch
  SpinLatch _allocing_list_latch[kAllocingListShard][kSlabSizeMax + 1];

  MemoryAccess *_access_table = nullptr;

  BufferPool *_buffer_pool = nullptr;

  RDMAClient *_client = nullptr;

  uint8_t _shard;

  SpinLock _lock;
  SpinLatch _latch;
  NetBuffer _net_buffer[kThreadNum];
  ibv_mr *_net_buffer_mr;
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
    if (succ) {
      LOG_INFO("Alloc block %d successfully, prepare response.", cur);
    } else {
      LOG_ERROR("Alloc block %d Failed", cur);
    }
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