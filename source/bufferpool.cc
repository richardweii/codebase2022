#include "bufferpool.h"
#include <infiniband/verbs.h>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include "block.h"
#include "config.h"
#include "rdma_conn_manager.h"
#include "stat.h"
#include "util/defer.h"
#include "util/filter.h"
#include "util/logging.h"

namespace kv {

static std::atomic<BlockId> blockId(1);

BlockId getGetBlockId() { return blockId.fetch_add(1, std::memory_order_relaxed); }

BufferPool::BufferPool(size_t size, uint8_t shard, ConnectionManager *conn_manager) {
  shard_ = shard;
  pool_size_ = size;
  datablocks_ = new DataBlock[size];
  handles_.reserve(size);
  for (size_t i = 0; i < size; i++) {
    handles_.emplace_back(new BlockHandle(&datablocks_[i]));
  }
  connection_manager_ = conn_manager;
  pd_ = conn_manager->Pd();
  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

bool BufferPool::Init() {
  auto mr = ibv_reg_mr(pd_, datablocks_, kDataBlockSize * pool_size_,
                       IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
  if (!mr) {
    LOG_ERROR("%d shard registrate datablock failed.", shard_);
    perror("ibv_reg_mr fail");
    return false;
  }
  mr_ = mr;
  return true;
}

DataBlock *BufferPool::GetNewDataBlock() {
  std::lock_guard<std::mutex> guard(mutex_);
  int ret;

  FrameId frame_id;
  if (!free_list_.empty()) {
    // find a free frame in free_list
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // find a free frame using LRU
    frame_id = pop();
    if (frame_id == INVALID_FRAME_ID) {
      return nullptr;
    }
    // erase old
    WriteLockTable();
    ret = block_table_.erase(datablocks_[frame_id].GetId());
    assert(ret == 1);
    WriteUnlockTable();

    // write back victim
    uint64_t addr;
    uint32_t rkey;
    LOG_DEBUG("Request new datablock from remote...");
    ret = connection_manager_->Alloc(shard_, addr, rkey, kDataBlockSize);
    LOG_DEBUG("addr %lx, rkey %x", addr, rkey);

    LOG_ASSERT(ret == 0, "Alloc Failed.");
    ret = connection_manager_->RemoteWrite(&datablocks_[frame_id], mr_->lkey, kDataBlockSize, addr, rkey);
    LOG_ASSERT(ret == 0, "Remote Write Datablock Failed.");
    LOG_DEBUG("Get new datablock from remote successfully...");
  }

  BlockId id = getGetBlockId();
  datablocks_[frame_id].Free();
  datablocks_[frame_id].SetId(id);

  // insert new
  WriteLockTable();
  block_table_[datablocks_[frame_id].GetId()] = frame_id;
  WriteUnlockTable();

  renew(frame_id);

  return &datablocks_[frame_id];
}

bool BufferPool::replacement(Slice key, FrameId &fid) {
  // lookup
  uint64_t read_addr;
  uint32_t read_rkey;
  bool found = false;
  int ret;
  ret = connection_manager_->Lookup(key, read_addr, read_rkey, found);
  if (!found) {
    stat::remote_miss.fetch_add(1, std::memory_order_relaxed);
    LOG_DEBUG("Cannot find %s at remote.", key.data());
    return found;
  }

  // replacement
  FrameId frame_id;
  frame_id = pop();
  if (frame_id == INVALID_FRAME_ID) {
    return false;
  }

  stat::replacement.fetch_add(1, std::memory_order_relaxed);

  // write back victim
  uint64_t write_addr;
  uint32_t write_rkey;
  LOG_DEBUG("Request new datablock for replace block %d", datablocks_[frame_id].GetId());
  ret = connection_manager_->Alloc(shard_, write_addr, write_rkey, kDataBlockSize);
  LOG_ASSERT(ret == 0, "Alloc Failed.");
  ret = connection_manager_->RemoteWrite(&datablocks_[frame_id], mr_->lkey, kDataBlockSize, write_addr, write_rkey);
  LOG_ASSERT(ret == 0, "Remote Write Datablock Failed.");

  // erase old
  WriteLockTable();
  ret = block_table_.erase(datablocks_[frame_id].GetId());
  datablocks_[frame_id].Free();
  assert(ret == 1);
  WriteUnlockTable();

  // fetch
  ret = connection_manager_->RemoteRead(&datablocks_[frame_id], mr_->lkey, kDataBlockSize, read_addr, read_rkey);
  LOG_ASSERT(ret == 0, "Remote Read Datablock Failed.");
  LOG_DEBUG("Read Block %d", datablocks_[frame_id].GetId());
  // insert new
  WriteLockTable();
  block_table_[datablocks_[frame_id].GetId()] = frame_id;
  WriteUnlockTable();

  // LRU update
  renew(frame_id);

  connection_manager_->Free(shard_, datablocks_[frame_id].GetId());
  LOG_DEBUG("Replacement finish.");

  fid = frame_id;
  return found;
}

void BufferPool::renew(FrameId frame_id) {
  if (frame_mapping_.count(frame_id) == 0) {
    // add to frame_mapping_
    Frame *new_frame = new Frame(frame_id);
    frame_mapping_.emplace(frame_id, new_frame);
    // add to frame_list
    if (frame_list_head_ == nullptr) {
      assert(frame_list_head_ == frame_list_tail_);
      frame_list_head_ = new_frame;
      frame_list_tail_ = new_frame;
    } else {
      frame_list_tail_->next = new_frame;
      new_frame->front = frame_list_tail_;
      frame_list_tail_ = new_frame;
    }
  } else {
    auto frame = frame_mapping_[frame_id];
    // remove from list
    if (frame->front == nullptr && frame->next == nullptr) {
      frame_list_head_ = nullptr;
      frame_list_tail_ = nullptr;
    } else if (frame->front == nullptr) {  // head
      frame_list_head_ = frame->next;
      frame_list_head_->front = nullptr;
    } else if (frame->next == nullptr) {  // tail
      frame_list_tail_ = frame->front;
      frame_list_tail_->next = nullptr;
    } else {
      frame->front->next = frame->next;
      frame->next->front = frame->front;
    }
    frame->front = nullptr;
    frame->next = nullptr;
    // add to frame_list
    if (frame_list_head_ == nullptr) {
      assert(frame_list_head_ == frame_list_tail_);
      frame_list_head_ = frame;
      frame_list_tail_ = frame;
    } else {
      frame_list_tail_->next = frame;
      frame->front = frame_list_tail_;
      frame_list_tail_ = frame;
    }
  }
}

FrameId BufferPool::pop() {
  if (frame_list_head_ == nullptr) {
    return INVALID_FRAME_ID;
  }

  // remove from list
  auto frame = frame_list_head_;
  frame_list_head_ = frame_list_head_->next;
  if (frame_list_head_ != nullptr) {
    frame_list_head_->front = nullptr;
  } else {
    frame_list_tail_ = nullptr;
  }
  // remove from hash_table
  // assert(frame_mapping_.count(frame->frame_) != 0);
  if (frame_mapping_.count(frame->frame_) == 0) {
    LOG_ERROR("fuck");
  }
  frame_mapping_.erase(frame->frame_);

  FrameId ret = frame->frame_;
  delete frame;
  return ret;
}

bool BufferPool::Read(Slice key, std::string &value, Ptr<Filter> filter, CacheEntry &entry) {
  std::lock_guard<std::mutex> guard(mutex_);

  for (auto &kv : block_table_) {
    FrameId fid = kv.second;

    BlockHandle *handle = handles_[fid];
    if (handle->Read(key, value, filter, entry)) {
      renew(fid);
      stat::local_access.fetch_add(1);
      return true;
    }
  }

  FrameId fid;
  // TODO: replacement can return off of entry, aviod additional lookup
  bool found = replacement(key, fid);
  if (!found) {
    return false;
  }

  bool succ = handles_[fid]->Read(key, value, filter, entry);
  LOG_ASSERT(succ, "Fetched invalid datablock.")
  return true;
}

bool BufferPool::Modify(Slice key, Slice value, Ptr<Filter> filter, CacheEntry &entry) {
  std::lock_guard<std::mutex> guard(mutex_);

  for (auto &kv : block_table_) {
    BlockId id = kv.first;
    FrameId fid = kv.second;

    BlockHandle *handle = handles_[fid];
    if (handle->Modify(key, value, filter, entry)) {
      stat::local_access.fetch_add(1);
      renew(fid);
      return true;
    }
  }

  FrameId fid;
  bool found = replacement(key, fid);
  if (!found) {
    return false;
  }

  bool succ = handles_[fid]->Modify(key, value, filter, entry);
  LOG_ASSERT(succ, "Fetched invalid datablock.")
  return true;
}

bool BufferPool::Fetch(Slice key, BlockId id) {
  std::lock_guard<std::mutex> lg(mutex_);
  LOG_DEBUG("Fetch block %d for key %s", id, key.data());

  if (block_table_.count(id)) {
    LOG_DEBUG("Other has fetched the block.");
    return true;
  }

  stat::fetch.fetch_add(1, std::memory_order_relaxed);
  // lookup
  uint64_t read_addr;
  uint32_t read_rkey;
  int ret;
  ret = connection_manager_->Fetch(shard_, id, read_addr, read_rkey);
  assert(ret == 0);

  // replacement
  FrameId frame_id;
  frame_id = pop();
  if (frame_id == INVALID_FRAME_ID) {
    return false;
  }

  // write back victim
  uint64_t write_addr;
  uint32_t write_rkey;
  LOG_DEBUG("Request new datablock for replace block %d", datablocks_[frame_id].GetId());
  ret = connection_manager_->Alloc(shard_, write_addr, write_rkey, kDataBlockSize);
  LOG_ASSERT(ret == 0, "Alloc Failed.");
  ret = connection_manager_->RemoteWrite(&datablocks_[frame_id], mr_->lkey, kDataBlockSize, write_addr, write_rkey);
  LOG_ASSERT(ret == 0, "Remote Write Datablock Failed.");

  // erase old
  WriteLockTable();
  ret = block_table_.erase(datablocks_[frame_id].GetId());
  datablocks_[frame_id].Free();
  assert(ret == 1);
  WriteUnlockTable();

  // fetch
  ret = connection_manager_->RemoteRead(&datablocks_[frame_id], mr_->lkey, kDataBlockSize, read_addr, read_rkey);
  LOG_ASSERT(ret == 0, "Remote Read Datablock Failed.");
  LOG_DEBUG("Read Block %d", datablocks_[frame_id].GetId());
  // insert new
  WriteLockTable();
  block_table_[datablocks_[frame_id].GetId()] = frame_id;
  WriteUnlockTable();

  // LRU update
  renew(frame_id);

  connection_manager_->Free(shard_, datablocks_[frame_id].GetId());
  LOG_DEBUG("Replacement finish.");

  LOG_ASSERT(datablocks_[frame_id].GetId() == id, "Unmatched block id.");
  LOG_ASSERT(handles_[frame_id]->Find(key, NewBloomFilterPolicy()), "Invalid key in cache.");
  LOG_ASSERT(HasBlock(id), "Failed to fetch block %d", id);
  return true;
}

}  // namespace kv