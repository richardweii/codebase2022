#include "pool.h"
#include <infiniband/verbs.h>
#include <cstddef>
#include <cstdint>
#include "block.h"
#include "bufferpool.h"
#include "cache.h"
#include "config.h"
#include "memtable.h"
#include "rdma_conn_manager.h"
#include "util/defer.h"
#include "util/filter.h"
#include "util/logging.h"

namespace kv {

Pool::Pool(size_t buffer_pool_size, size_t filter_bits, size_t cache_size, uint8_t shard,
           ConnectionManager *conn_manager) {
  buffer_pool_ = new BufferPool(buffer_pool_size, shard, conn_manager);
  filter_length_ = (filter_bits + 7)/ 8;
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

Value Pool::Read(Key key, Ptr<Filter> filter) {
  latch_.RLock();
  defer { latch_.RUnlock(); };
  Value val;
  if (!filter->KeyMayMatch(Slice(key->c_str(), key->size()), Slice(filter_data_, filter_length_))) {
    return nullptr;
  }
  if ((val = memtable_->Read(key)) != nullptr) {
    return val;
  }

  auto cache_handle = cache_->Lookup(Slice(key->c_str(), key->size()));
  if (cache_handle != nullptr) {
    auto entry = (CacheEntry *)cache_->Value(cache_handle);
    defer { cache_->Release(cache_handle); };

    buffer_pool_->ReadLockTable();
    BlockHandle *handle = entry->handle;

    if (buffer_pool_->HasBlock(entry->id)) {
      val = handle->Read(entry->off);
      return val;
    } else {
      // cache invalid, need fetch the datablock
      buffer_pool_->ReadUnlockTable();
      buffer_pool_->Fetch(key, entry->id);
      LOG_ASSERT(handle->GetBlockId() == entry->id, "Invalid handle.");
      val = handle->Read(entry->off);
      return val;
    }
  } else {
    // cache miss
    CacheEntry *entry = new CacheEntry();
    val = buffer_pool_->Read(key, filter, *entry);
    if (val != nullptr) {
      auto handle = cache_->Insert(Slice(key->c_str(), key->size()), entry, sizeof(CacheEntry), CacheDeleter);
      cache_->Release(handle);
    }
    return val;
  }
}

void Pool::insertIntoMemtable(Key key, Value val, Ptr<Filter> filter) {
  if (memtable_->Full()) {
    DataBlock *block = buffer_pool_->GetNewDataBlock();
    // LOG_DEBUG("memtable is full, get new block %d", block->GetId());
    memtable_->BuildDataBlock(block);
    memtable_->Reset();
  }
  memtable_->Insert(key, val);
  Slice s(key->c_str(), key->size());
  filter->AddFilter(s, this->filter_length_ * 8, filter_data_);
}

bool Pool::Write(Key key, Value val, Ptr<Filter> filter) {
  latch_.WLock();
  defer { latch_.WUnlock(); };
  if (!filter->KeyMayMatch(Slice(key->c_str(), key->size()), Slice(filter_data_, filter_length_))) {
    insertIntoMemtable(key, val, filter);
    return true;
  }

  if (memtable_->Read(key) != nullptr) {
    memtable_->Insert(key, val);
    return true;
  }

  auto cache_handle = cache_->Lookup(Slice(key->c_str(), key->size()));
  bool ret = false;
  if (cache_handle != nullptr) {
    auto entry = (CacheEntry *)cache_->Value(cache_handle);
    defer { cache_->Release(cache_handle); };

    BlockHandle *handle = entry->handle;
    if (buffer_pool_->HasBlock(entry->id)) {
      ret = handle->Modify(entry->off, val);
      LOG_ASSERT(ret, "Invalid cache entry %s.", key->c_str());
      return true;
    } else {
      // cache invalid, need fetch the datablock
      buffer_pool_->Fetch(key, entry->id);
      LOG_ASSERT(handle->GetBlockId() == entry->id, "Invalid handle.");
      ret = handle->Modify(entry->off, val);
      LOG_ASSERT(ret, "Invalid cache entry %s.", key->c_str());
      return true;
    }
  } else {
    // cache miss
    CacheEntry *entry = new CacheEntry();
    ret = buffer_pool_->Modify(key, val, filter, *entry);
    if (ret) {
      // in archive
      auto handle = cache_->Insert(Slice(key->c_str(), key->size()), entry, sizeof(CacheEntry), CacheDeleter);
      cache_->Release(handle);
    } else {
      insertIntoMemtable(key, val, filter);
    }
    return true;
  }
}

FrameId RemotePool::findBlock(BlockId id) const {
  for (int i = 0; i < handles_.size(); i++) {
    if (handles_[i]->GetBlockId() == id) {
      return i;
    }
  }
  return INVALID_FRAME_ID;
}

bool RemotePool::FreeDataBlock(BlockId id) {
  latch_.WLock();
  defer { latch_.WUnlock(); };
  FrameId fid = findBlock(id);
  if (fid == INVALID_FRAME_ID) {
    // does not exist
    LOG_ERROR("The block %d to be freed not exists.", id);
    return false;
  }
  getDataBlock(fid)->Free();
  free_list_.push_back(fid);
  return true;
}

RemotePool::MemoryAccess RemotePool::AllocDataBlock() {
  latch_.WLock();
  defer { latch_.WUnlock(); };
  if (!free_list_.empty()) {
    FrameId fid = free_list_.front();
    free_list_.pop_front();
    static_assert(sizeof(DataBlock *) == sizeof(uint64_t), "Pointer should be 8 bytes.");
    LOG_DEBUG("addr %lx, rkey %x", (uint64_t)getDataBlock(fid), getMr(fid)->rkey);
    return {(uint64_t)getDataBlock(fid), getMr(fid)->rkey};
  }

  // need allocate new frame
  constexpr int num = kRemoteMrSize / kDataBlockSize;
  datablocks_.emplace_back(new MR());
  FrameId cur = handles_.size();
  for (int i = 0; i < num; i++) {
    handles_.emplace_back(new BlockHandle(&datablocks_.back()->data[i]));
  }
  for (FrameId id = cur + 1; id < handles_.size(); id++) {
    free_list_.push_back(id);
  }
  auto mr = ibv_reg_mr(pd_, datablocks_.back(), kRemoteMrSize, RDMA_MR_FLAG);
  LOG_ASSERT(mr != nullptr, "Registrate new datablock failed.");
  mr_.push_back(mr);
  LOG_DEBUG("Registrate %d datablock", num);
  return {(uint64_t)datablocks_.back(), mr_.back()->rkey};
}

RemotePool::MemoryAccess RemotePool::AccessDataBlock(BlockId id) const {
  latch_.RLock();
  defer { latch_.RUnlock(); };
  FrameId fid = findBlock(id);
  if (fid == INVALID_FRAME_ID) {
    LOG_FATAL("Invalid block id %d", id);
    return {};
  }

  return {(uint64_t)getDataBlock(fid), getMr(fid)->rkey};
}

BlockId RemotePool::Lookup(Key key, Ptr<Filter> filter) const {
  latch_.RLock();
  defer { latch_.RUnlock(); };
  CacheEntry entry;  // not used
  for (auto &handle : handles_) {
    if (handle->Read(key, filter, entry) != nullptr) {
      return handle->GetBlockId();
    }
  }
  return INVALID_BLOCK_ID;
}

}  // namespace kv