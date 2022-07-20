#pragma once

#include <infiniband/verbs.h>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include "block.h"
#include "bufferpool.h"
#include "cache.h"
#include "config.h"
#include "hash_table.h"
#include "memtable.h"
#include "rdma_client.h"
#include "util/filter.h"
#include "util/logging.h"
#include "util/nocopy.h"
#include "util/rwlock.h"
#include "util/slice.h"

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
  Pool(size_t buffer_pool_size, size_t filter_bits, size_t cache_size, uint8_t shard, RDMAClient *client);
  ~Pool();

  void Init() {
    if (!buffer_pool_->Init()) {
      LOG_ERROR("Init bufferpool failed.");
    }
    filter_ = NewBloomFilterPolicy();
  }

  bool Read(Slice key, std::string &val);
  bool Write(Slice key, Slice val);

 private:
  void insertIntoMemtable(Slice key, Slice val);
  char *filter_data_;
  int filter_length_;

  MemTable *memtable_;
  RDMAClient *client_;
  BufferPool *buffer_pool_;
  Filter *filter_;
  Cache *cache_;
  Latch latch_;
};

class RemotePool NOCOPYABLE {
 public:
  class RemotePoolHashHandler : public HashHandler<Slice> {
   public:
    RemotePoolHashHandler(RemotePool *pool) : pool_(pool){};
    Slice GetKey(uint64_t data_handle) override {
      FrameId fid = data_handle >> 32;
      int off = data_handle << 32 >> 32;
      auto handle = pool_->handles_[fid];
      return Slice(handle->ReadEntry(off)->key, kKeyLength);
    };

    FrameId GetFrameId(uint64_t data_handle) {
      FrameId fid = data_handle >> 32;
      return fid;
    };

    Entry *GetEntry(uint64_t data_handle) {
      FrameId fid = GetFrameId(data_handle);
      int off = data_handle << 32 >> 32;
      auto handle = pool_->handles_[fid];
      return handle->ReadEntry(off);
    }

    uint64_t GenHandle(FrameId fid, int off) { return (uint64_t)fid << 32 | off; }

    RemotePool *pool_;
  };
  struct MR {
    DataBlock data[kRemoteMrSize / kDataBlockSize];
  };
  RemotePool(ibv_pd *pd, uint8_t shard) : pd_(pd), shard_(shard) {
    handler_ = new RemotePoolHashHandler(this);
    hash_table_ = new HashTable<Slice>(kKeyNum / kPoolShardNum, handler_);
  }
  ~RemotePool() {
    delete hash_table_;
    for (auto &ptr : handles_) {
      delete ptr;
    }
    for (auto &ptr : datablocks_) {
      delete ptr;
    }
    delete handler_;
  }
  // Allocate a datablock for one side write
  MemoryAccess AllocDataBlock(BlockId bid);

  // Access to a valid datablock for coming one side read
  MemoryAccess AccessDataBlock(BlockId id) const;

  // Lookup the key in the pool
  BlockId Lookup(Slice key) const;

  void CreateIndex(BlockId id);

 private:
  FrameId findBlock(BlockId id) const;

  void indexRountine();

  DataBlock *getDataBlock(FrameId id) const {
    constexpr int ratio = kRemoteMrSize / kDataBlockSize;
    int index = id / ratio;
    int off = id % ratio;
    assert(index < (int)datablocks_.size());
    return &datablocks_[index]->data[off];
  }

  ibv_mr *getMr(FrameId id) const {
    constexpr int ratio = kRemoteMrSize / kDataBlockSize;
    int index = id / ratio;
    assert(index < (int)mr_.size());
    return mr_[index];
  }

  RemotePoolHashHandler *handler_ = nullptr;

  std::vector<ibv_mr *> mr_;
  std::vector<MR *> datablocks_;

  ibv_pd *pd_ = nullptr;

  std::unordered_map<BlockId, FrameId> block_table_;
  std::vector<BlockHandle *> handles_;

  HashTable<Slice> *hash_table_ = nullptr;
  std::list<FrameId> free_list_;
  uint8_t shard_;
  mutable Latch latch_;
};

}  // namespace kv