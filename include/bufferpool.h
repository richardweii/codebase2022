#pragma once

#include <infiniband/verbs.h>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "block.h"
#include "config.h"
#include "hash_table.h"
#include "rdma_client.h"
#include "util/logging.h"
#include "util/nocopy.h"
#include "util/rwlock.h"
#include "util/slice.h"

namespace kv {

class BufferPool NOCOPYABLE {
 public:
  class BufferPoolHashHandler : public HashHandler {
   public:
    BufferPoolHashHandler(BufferPool *buffer_pool) : buffer_pool_(buffer_pool){};
    Slice GetKey(uint64_t data_handle) override {
      BlockId bid = data_handle >> 32;
      int off = data_handle << 32 >> 32;

      LOG_ASSERT(buffer_pool_->block_table_.count(bid) == 1, "invalid block id %d", bid);
      FrameId fid = buffer_pool_->block_table_[bid];
      auto handle = buffer_pool_->handles_[fid];
      return Slice(handle->Read(off)->key, kKeyLength);
    };

    FrameId GetFrameId(uint64_t data_handle) {
      BlockId bid = data_handle >> 32;
      LOG_ASSERT(buffer_pool_->block_table_.count(bid) == 1, "invalid block id %d", bid);
      FrameId fid = buffer_pool_->block_table_[bid];
      return fid;
    };

    Entry *GetEntry(uint64_t data_handle) {
      FrameId fid = GetFrameId(data_handle);
      int off = data_handle << 32 >> 32;
      auto handle = buffer_pool_->handles_[fid];
      return handle->Read(off);
    }

    BufferPool *buffer_pool_;
  };

  BufferPool(size_t size, uint8_t shard, RDMAClient *client);
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
    delete hash_table_;
    delete handler_;
  }

  // init local memory regestration
  bool Init();

  // get a new datablock for local write
  DataBlock *GetNewDataBlock();

  // read the value by key, return empty slice if not found
  bool Read(Slice key, std::string &value);

  // modify the value if the key exists
  bool Modify(Slice key, Slice value);

  void CreateIndex(DataBlock *block) {
    std::lock_guard<std::mutex> guard(mutex_);
    FrameId fid = block_table_[block->GetId()];
    assert(fid != INVALID_FRAME_ID);
    createIndex(handles_[fid]);
  }

 private:
  bool alloc(uint8_t shard, BlockId id, uint64_t &addr, uint32_t &rkey);

  bool lookup(Slice slice, uint64_t &addr, uint32_t &rkey);
  // write back one datablock for fetching another one from remote if the block holding the key exists.
  bool replacement(Slice key, FrameId &fid);

  bool remoteCreateIndex(uint8_t shard, BlockId id);

  void prune(BlockHandle *handle);

  void createIndex(BlockHandle *handle);

  DataBlock *datablocks_ = nullptr;
  std::vector<BlockHandle *> handles_;

  RDMAClient *client_ = nullptr;
  ibv_pd *pd_ = nullptr;
  ibv_mr *mr_ = nullptr;  // one mr

  BufferPoolHashHandler *handler_;
  uint8_t shard_;

  HashTable *hash_table_;
  std::unordered_map<BlockId, FrameId> block_table_;
  std::unordered_map<BlockId, MemoryAccess> global_table_;

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