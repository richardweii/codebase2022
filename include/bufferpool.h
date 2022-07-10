#pragma once

#include <infiniband/verbs.h>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>
#include "block.h"
#include "config.h"
#include "rdma_conn_manager.h"
#include "util/nocopy.h"
#include "util/rwlock.h"
#include "util/slice.h"

namespace kv {

class BufferPool NOCOPYABLE {
 public:
  BufferPool(size_t size, uint8_t shard, ConnectionManager *conn_manager);
  ~BufferPool() {
    auto ret = ibv_dereg_mr(mr_);
    delete[] datablocks_;
    for (auto &handle : handles_) {
      delete handle;
    }
    assert(ret == 0);
    for (auto &kv : frame_mapping_) {
      delete kv.second;
    }
  }

  // init local memory regestration
  bool Init();

  // get a new datablock for local write
  DataBlock *GetNewDataBlock();

  // read the value by key, return empty slice if not found
  Value Read(Key key, Ptr<Filter> filter, CacheEntry &entry);

  // when a cache entry point to a invalid datablock, need fetch the block from remote.
  bool Fetch(Key key, BlockId id);

  // modify the value if the key exists
  bool Modify(Key key, Value val, Ptr<Filter> filter, CacheEntry &entry);

  // not thread-safe, need protect block_table_
  bool HasBlock(BlockId id) const { return block_table_.count(id); }

  // not thread-safe, need protect block_table_
  BlockHandle *GetHandle(BlockId id) const {
    if (block_table_.count(id) == 0){
      return nullptr;
    }
    return handles_.at(block_table_.at(id));
  }

  void ReadLockTable() { table_latch_.RLock(); }
  void ReadUnlockTable() { table_latch_.RUnlock(); };
  void WriteLockTable() { table_latch_.WLock(); }
  void WriteUnlockTable() { table_latch_.WUnlock(); };

 private:
  // write back one datablock for fetching another one from remote if the block holding the key exists.
  bool replacement(Key key, FrameId &fid);

  DataBlock *datablocks_ = nullptr;
  std::vector<BlockHandle *> handles_;

  ConnectionManager *connection_manager_;
  ibv_pd *pd_ = nullptr;
  ibv_mr *mr_ = nullptr;  // one mr

  uint8_t shard_;

  std::unordered_map<BlockId, FrameId> block_table_;
  Latch table_latch_;

  std::list<FrameId> free_list_;
  size_t pool_size_;
  std::mutex mutex_;

  /**
   * LRU 实现
   */
  struct Frame {
    explicit Frame(FrameId frame_id) : frame_(frame_id), next(nullptr), front(nullptr) {}
    FrameId frame_;
    Frame *next;
    Frame *front;
  };
  Frame *frame_list_head_ = nullptr;
  Frame *frame_list_tail_ = nullptr;
  std::unordered_map<FrameId, Frame *> frame_mapping_;
  void renew(FrameId frame_id);
  FrameId pop();
};
}  // namespace kv