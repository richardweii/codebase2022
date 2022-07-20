#include "pool.h"
#include <atomic>
#include <cassert>
#include <cstdint>
#include <thread>
#include "cache.h"
#include "config.h"
#include "rdma_manager.h"
#include "stat.h"
#include "util/defer.h"
#include "util/logging.h"

namespace kv {

Pool::Pool(size_t buffer_pool_size, size_t filter_bits, size_t cache_size, uint8_t shard, RDMAClient *client) {
  buffer_pool_ = new BufferPool(buffer_pool_size, shard, client);
  filter_length_ = (filter_bits + 7) / 8;
  filter_data_ = new char[filter_length_];
  memtable_ = new MemTable();
  cache_ = NewLRUCache(cache_size);
}

Pool::~Pool() {
  delete buffer_pool_;
  delete memtable_;
  delete cache_;
  delete[] filter_data_;
}

bool Pool::Read(Slice key, std::string &val) {
  bool cache_invalid = false;
  CacheEntry read_entry;
  {
    latch_.RLock();
    defer { latch_.RUnlock(); };
    if (!filter_->KeyMayMatch(key, Slice(filter_data_, filter_length_))) {
      return false;
    }
    if (memtable_->Read(key, val)) {
      return true;
    }

    auto cache_handle = cache_->Lookup(key);
    if (cache_handle != nullptr) {
      read_entry = cache_->Value(cache_handle);
      defer { cache_->Release(cache_handle); };

      buffer_pool_->ReadLockTable();
      if (buffer_pool_->HasBlock(read_entry.id)) {
        BlockHandle *handle = buffer_pool_->GetHandle(read_entry.id);
        handle->Read(read_entry.off, val);
        buffer_pool_->ReadUnlockTable();
        stat::cache_hit.fetch_add(1, std::memory_order_relaxed);
        return true;
      } else {
        // cache invalid, need fetch the datablock
        cache_invalid = true;
        buffer_pool_->ReadUnlockTable();
      }
    } else {
      // cache miss
      CacheEntry new_entry;
      auto succ = buffer_pool_->Read(key, val, new_entry);
      if (succ) {
        auto handle = cache_->Insert(key, std::move(new_entry));
        cache_->Release(handle);
        return true;
      }
    }
  }
  {
    latch_.WLock();
    defer { latch_.WUnlock(); };
    if (cache_invalid) {
    retry:
      buffer_pool_->MissFetch(key, read_entry.id);
      BlockHandle *handle = buffer_pool_->GetHandle(read_entry.id);
      if (handle == nullptr) {
        // the block fetched have been evicted
        goto retry;
      }
      LOG_ASSERT(handle->GetBlockId() == read_entry.id, "Invalid handle. expected %d, got %d", handle->GetBlockId(),
                 read_entry.id);
      stat::cache_invalid.fetch_add(1, std::memory_order_relaxed);
      handle->Read(read_entry.off, val);
      return true;
    } else {
      CacheEntry new_entry;
      auto succ = buffer_pool_->FetchRead(key, val, new_entry);
      if (succ) {
        auto handle = cache_->Insert(key, std::move(new_entry));
        cache_->Release(handle);
      }
      return succ;
    }
  }
}

void Pool::insertIntoMemtable(Slice key, Slice val) {
  stat::insert_num.fetch_add(1, std::memory_order_relaxed);
  if (stat::insert_num.load() == 160000001) {
    LOG_INFO("finish insert.");
  }
  if (memtable_->Full()) {
    stat::block_num.fetch_add(1, std::memory_order_relaxed);
    DataBlock *block = buffer_pool_->GetNewDataBlock();
    // LOG_DEBUG("memtable is full, get new block %d", block->GetId());
    memtable_->BuildDataBlock(block);
    ;
    memtable_->Reset();
  }
  memtable_->Insert(key, val);
  filter_->AddFilter(key, this->filter_length_ * 8, filter_data_);
}

bool Pool::Write(Slice key, Slice val) {
  latch_.WLock();
  defer { latch_.WUnlock(); };
  if (!filter_->KeyMayMatch(key, Slice(filter_data_, filter_length_))) {
    insertIntoMemtable(key, val);
    return true;
  }

  if (memtable_->Exist(key)) {
    memtable_->Insert(key, val);
    return true;
  }

  auto cache_handle = cache_->Lookup(key);
  bool ret = false;
  if (cache_handle != nullptr) {
    auto &entry = cache_->Value(cache_handle);
    defer { cache_->Release(cache_handle); };

    if (buffer_pool_->HasBlock(entry.id)) {
      BlockHandle *handle = buffer_pool_->GetHandle(entry.id);
      ret = handle->Modify(entry.off, val);
      LOG_ASSERT(ret, "Invalid cache entry %s.", key.data());
      stat::cache_hit.fetch_add(1, std::memory_order_relaxed);
      return true;
    } else {
      // cache invalid, need fetch the datablock
      buffer_pool_->MissFetch(key, entry.id);
      BlockHandle *handle = buffer_pool_->GetHandle(entry.id);
      LOG_ASSERT(handle->GetBlockId() == entry.id, "Invalid handle.");
      ret = handle->Modify(entry.off, val);
      LOG_ASSERT(ret, "Invalid cache entry %s.", key.data());
      stat::cache_invalid.fetch_add(1, std::memory_order_relaxed);
      return true;
    }
  } else {
    // cache miss
    CacheEntry entry;
    ret = buffer_pool_->Modify(key, val, entry);
    if (ret) {
      // in archive
      auto handle = cache_->Insert(key, std::move(entry));
      cache_->Release(handle);
    } else {
      insertIntoMemtable(key, val);
    }
    return true;
  }
}

FrameId RemotePool::findBlock(BlockId id) const {
  if (block_table_.count(id)) {
    return block_table_.at(id);
  }
  return INVALID_FRAME_ID;
}

MemoryAccess RemotePool::AllocDataBlock(BlockId bid) {
  latch_.WLock();
  defer { latch_.WUnlock(); };
  if (!free_list_.empty()) {
    FrameId fid = free_list_.front();
    free_list_.pop_front();
    static_assert(sizeof(void *) == sizeof(uint64_t), "Pointer should be 8 bytes.");
    block_table_[bid] = fid;
    frame_count_.fetch_add(1);
    return {(uint64_t)getDataBlock(fid), getMr(fid)->rkey};
  }

  // need allocate new frame
  constexpr int num = kRemoteMrSize / kDataBlockSize;
  datablocks_.emplace_back(new MR());
  FrameId cur = handles_.size();
  for (int i = 0; i < num; i++) {
    handles_.emplace_back(new BlockHandle(&datablocks_.back()->data[i]));
  }
  for (FrameId id = cur + 1; id < (FrameId)handles_.size(); id++) {
    free_list_.push_back(id);
  }
  auto mr = ibv_reg_mr(pd_, datablocks_.back(), kRemoteMrSize, RDMA_MR_FLAG);
  LOG_ASSERT(mr != nullptr, "Registrate new datablock failed.");
  mr_.push_back(mr);
  LOG_DEBUG("Registrate %d datablock", num);
  block_table_[bid] = cur;
  frame_count_.fetch_add(1);
  return {(uint64_t)datablocks_.back()->data, mr_.back()->rkey};
}

MemoryAccess RemotePool::AccessDataBlock(BlockId id) const {
  latch_.RLock();
  defer { latch_.RUnlock(); };
  FrameId fid = findBlock(id);
  if (fid == INVALID_FRAME_ID) {
    LOG_FATAL("Invalid block id %d", id);
    return {};
  }

  return {(uint64_t)getDataBlock(fid), getMr(fid)->rkey};
}

BlockId RemotePool::Lookup(Slice key) const {
  latch_.RLock();
  defer { latch_.RUnlock(); };
  auto node = hash_table_->Find(key);
  if (node == nullptr) {
    return INVALID_BLOCK_ID;
  }

  return handles_[handler_->GetFrameId(node->Handle())]->GetBlockId();
}

void RemotePool::indexRountine() {
  int cur = 0;
  while (!stop_) {
    while (cur <= frame_count_) {
      if (handles_[cur]->Valid()) {
        CreateIndex(handles_[cur]->GetBlockId());
        cur++;
      }
    }
    std::this_thread::yield();
  }
}

void RemotePool::CreateIndex(BlockId id) {
  LOG_INFO("shard %d create index of block %d", shard_, id);
  latch_.WLock();
  defer { latch_.WUnlock(); };
  FrameId fid = findBlock(id);
  assert(fid != INVALID_FRAME_ID);
  auto handle = handles_[fid];
  auto count = hash_table_->Count();
  for (uint32_t i = 0; i < handle->EntryNum(); i++) {
    uint64_t data_handle = handler_->GenHandle(fid, i);
    hash_table_->Insert(Slice(handle->ReadEntry(i)->key, kKeyLength), data_handle);
  }
  LOG_ASSERT(hash_table_->Count() - count == kItemNum, "Less than expected entries inserted. expected %d, got %lu",
             kItemNum, hash_table_->Count() - count);
}
}  // namespace kv