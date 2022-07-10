#pragma once

#include <infiniband/verbs.h>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>
#include "block.h"
#include "bufferpool.h"
#include "cache.h"
#include "config.h"
#include "memtable.h"
#include "rdma_conn_manager.h"
#include "util/filter.h"
#include "util/logging.h"
#include "util/nocopy.h"
#include "util/rwlock.h"

namespace kv {

class Pool NOCOPYABLE {
 public:
  /**
   * @brief Construct a new Pool object
   *
   * @param buffer_pool_size how many datablock in bufferpool
   * @param filter_bits bloom filter bits num 10 * key
   * @param cache_size cache data size
   */
  Pool(size_t buffer_pool_size, size_t filter_bits, size_t cache_size, uint8_t shard, ConnectionManager *conn_manager);
  ~Pool();

  void Init() {
    if (!buffer_pool_->Init()) {
      LOG_ERROR("Init bufferpool failed.");
    }
  }

  Value Read(Key key, Ptr<Filter> filter);
  bool Write(Key key, Value val, Ptr<Filter> filter);

 private:
  void insertIntoMemtable(Key key, Value val, Ptr<Filter> filter);
  char *filter_data_;
  int filter_length_;

  MemTable *memtable_;
  Cache *cache_;
  BufferPool *buffer_pool_;
  Latch latch_;
};

class RemotePool NOCOPYABLE {
 public:
  struct MemoryAccess {
    uint64_t data;
    uint32_t key;
  };
  struct MR {
    DataBlock data[kRemoteMrSize / kDataBlockSize];
  };
  RemotePool(ibv_pd *pd, uint8_t shard) : pd_(pd), shard_(shard) {}
  // Free a datablock have been fetched from local node
  bool FreeDataBlock(BlockId id);

  // Allocate a datablock for one side write
  MemoryAccess AllocDataBlock();

  // Access to a valid datablock for coming one side read
  MemoryAccess AccessDataBlock(BlockId id) const;

  // Lookup the key in the pool
  BlockId Lookup(Key key, Ptr<Filter> filter) const;

 private:
  FrameId findBlock(BlockId id) const;
  DataBlock *getDataBlock(FrameId id) const {
    constexpr int ratio = kRemoteMrSize / kDataBlockSize;
    int index = id / ratio;
    int off = id % ratio;
    assert(index < datablocks_.size());
    return &datablocks_[index]->data[off];
  }

  ibv_mr *getMr(FrameId id) const {
    constexpr int ratio = kRemoteMrSize / kDataBlockSize;
    int index = id / ratio;
    assert(index < mr_.size());
    return mr_[index];
  }

  std::vector<ibv_mr *> mr_;
  std::vector<BlockHandle *> handles_;
  std::vector<MR *> datablocks_;
  std::list<FrameId> free_list_;
  ibv_pd *pd_;
  uint8_t shard_;
  mutable Latch latch_;
};

}  // namespace kv