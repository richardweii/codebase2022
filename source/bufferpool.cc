#include "bufferpool.h"
#include <infiniband/verbs.h>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mutex>
#include "block.h"
#include "config.h"
#include "hash_table.h"
#include "msg.h"
#include "stat.h"
#include "util/defer.h"
#include "util/filter.h"
#include "util/logging.h"

namespace kv {

static std::atomic<BlockId> blockId(1);
static std::atomic<uint32_t> rid_counter(1);

BlockId getGetBlockId() { return blockId.fetch_add(1, std::memory_order_relaxed); }
uint32_t getRid() { return rid_counter.fetch_add(1, std::memory_order_relaxed); }

BufferPool::BufferPool(size_t size, uint8_t shard, RDMAClient *client) {
  shard_ = shard;
  pool_size_ = size;
  datablocks_ = new DataBlock[size];
  handles_.reserve(size);
  for (size_t i = 0; i < size; i++) {
    handles_.emplace_back(new BlockHandle(&datablocks_[i]));
  }
  handler_ = new BufferPoolHashHandler(this);
  hash_table_ = new HashTable<Slice>(size * kItemNum, handler_);
  client_ = client;
  pd_ = client->Pd();
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
  int ret;
  bool succ;

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
    BlockId old_bid = datablocks_[frame_id].GetId();

    // write victim to remote
    uint64_t addr;
    uint32_t rkey;
    bool alloced = false;
    if (!global_table_.count(old_bid)) {
      LOG_DEBUG("Request new datablock from remote...");
      succ = alloc(shard_, old_bid, addr, rkey);
      LOG_DEBUG("write block %d to addr %lx, rkey %x", old_bid, addr, rkey);
      global_table_[old_bid].addr = addr;
      global_table_[old_bid].rkey = rkey;
      LOG_ASSERT(succ, "Alloc Failed.");
      alloced = true;
    }
    addr = global_table_[old_bid].addr;
    rkey = global_table_[old_bid].rkey;

    prune(handles_[frame_id]);
    ret = client_->RemoteWrite(&datablocks_[frame_id], mr_->lkey, kDataBlockSize, addr, rkey);
    if (alloced) {
      succ = remoteCreateIndex(shard_, old_bid);
      LOG_ASSERT(succ, "Remote create index failed.");
    }
    LOG_ASSERT(ret == 0, "Remote Write Datablock Failed.");
    LOG_DEBUG("Get new datablock from remote successfully...");
  }

  BlockId id = getGetBlockId();
  datablocks_[frame_id].Free();
  datablocks_[frame_id].SetId(id);

  // insert new
  block_table_[datablocks_[frame_id].GetId()] = frame_id;

  renew(frame_id);

  return &datablocks_[frame_id];
}

bool BufferPool::alloc(uint8_t shard, BlockId id, uint64_t &addr, uint32_t &rkey) {
  AllocRequest req;
  req.shard = shard;
  req.size = kDataBlockSize;
  req.type = MSG_ALLOC;
  req.bid = id;
  req.rid = getRid();

  AllocResponse resp;
  auto ret = client_->RPC(req, resp);
  assert(ret == 0);
  assert(resp.status == RES_OK);
  addr = resp.addr;
  rkey = resp.rkey;
  return true;
}

bool BufferPool::lookup(Slice slice, uint64_t &addr, uint32_t &rkey) {
  LookupRequest req;
  req.type = MSG_LOOKUP;
  memcpy(req.key, slice.data(), slice.size());
  req.rid = getRid();

  LookupResponse resp;
  auto ret = client_->RPC(req, resp);
  assert(ret == 0);
  if (resp.status == RES_OK) {
    addr = resp.addr;
    rkey = resp.rkey;
    return true;
  }
  return false;
}

bool BufferPool::remoteCreateIndex(uint8_t shard, BlockId id) {
  CreateIndexRequest req;
  req.type = MSG_CREATE;
  req.rid = getRid();
  req.id = id;
  req.shard = shard;

  CreateIndexResponse resp;
  auto ret = client_->RPC(req, resp);
  assert(ret == 0);
  if (resp.status == RES_OK) {
    return true;
  }
  return false;
}

bool BufferPool::replacement(Slice key, FrameId &fid) {
  // lookup
  uint64_t read_addr;
  uint32_t read_rkey;
  bool found = false;
  int ret;
  bool succ;
  found = lookup(key, read_addr, read_rkey);
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

  BlockId victim = datablocks_[frame_id].GetId();

  stat::replacement.fetch_add(1, std::memory_order_relaxed);

  // write victim
  uint64_t write_addr;
  uint32_t write_rkey;
  bool alloced = false;
  if (!global_table_.count(victim)) {
    LOG_DEBUG("Request new datablock for replace block %d", victim);
    succ = alloc(shard_, victim, write_addr, write_rkey);
    LOG_ASSERT(succ, "Alloc Failed.");
    global_table_[victim].addr = write_addr;
    global_table_[victim].rkey = write_rkey;
    alloced = true;
  }
  write_addr = global_table_[victim].addr;
  write_rkey = global_table_[victim].rkey;

  prune(handles_[frame_id]);
  ret = block_table_.erase(victim);
  assert(ret == 1);

  ret = client_->RemoteWrite(&datablocks_[frame_id], mr_->lkey, kDataBlockSize, write_addr, write_rkey);
  LOG_ASSERT(ret == 0, "Remote Write Datablock Failed.");
  if (alloced) {
    succ = remoteCreateIndex(shard_, victim);
    LOG_ASSERT(succ, "Remote create index failed.");
  }

  // erase old
  datablocks_[frame_id].Free();

  // fetch
  ret = client_->RemoteRead(&datablocks_[frame_id], mr_->lkey, kDataBlockSize, read_addr, read_rkey);
  LOG_ASSERT(ret == 0, "Remote Read Datablock Failed.");
  LOG_DEBUG("Read Block %d", datablocks_[frame_id].GetId());

  block_table_[datablocks_[frame_id].GetId()] = frame_id;
  createIndex(handles_[frame_id]);

  // LRU update
  renew(frame_id);

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

bool BufferPool::FetchRead(Slice key, std::string &value) {
  auto node = hash_table_->Find(key);
  if (node != nullptr) {
    // renew(handler_->GetFrameId(node->Handle()));
    auto ent = handler_->GetEntry(node->Handle());
    value.resize(kValueLength);
    memcpy((char *)value.c_str(), ent->value, kValueLength);
    return true;
  }

  FrameId fid;
  bool found = replacement(key, fid);
  if (!found) {
    return false;
  }

  node = hash_table_->Find(key);
  LOG_ASSERT(node != nullptr, "Fetched invalid datablock.");
  auto ent = handler_->GetEntry(node->Handle());
  value.resize(kValueLength);
  memcpy((char *)value.c_str(), ent->value, kValueLength);
  return true;
}

bool BufferPool::Read(Slice key, std::string &value) {
  auto node = hash_table_->Find(key);
  if (node != nullptr) {
    // renew(handler_->GetFrameId(node->Handle()));
    auto ent = handler_->GetEntry(node->Handle());
    value.resize(kValueLength);
    memcpy((char *)value.c_str(), ent->value, kValueLength);
    return true;
  }

  return false;
}

bool BufferPool::Modify(Slice key, Slice value) {
  auto node = hash_table_->Find(key);
  if (node != nullptr) {
    // renew(handler_->GetFrameId(node->Handle()));
    auto ent = handler_->GetEntry(node->Handle());
    memcpy(ent->value, value.data(), kValueLength);
    return true;
  }

  FrameId fid;
  bool found = replacement(key, fid);
  if (!found) {
    return false;
  }

  node = hash_table_->Find(key);
  LOG_ASSERT(node != nullptr, "Fetched invalid datablock.");
  auto ent = handler_->GetEntry(node->Handle());
  memcpy(ent->value, value.data(), kValueLength);
  return true;
}

void BufferPool::prune(BlockHandle *handle) {
  auto count = hash_table_->Count();
  for (uint32_t i = 0; i < handle->EntryNum(); i++) {
    FrameId fid = block_table_[handle->GetBlockId()];
    assert(fid != INVALID_FRAME_ID);
    hash_table_->Remove(Slice(handle->Read(i)->key, kKeyLength));
  }
  LOG_ASSERT(count - hash_table_->Count() == kItemNum, "Less than expected entries removed. expected %d, got %lu",
             kItemNum, count - hash_table_->Count());
}

void BufferPool::createIndex(BlockHandle *handle) {
  auto count = hash_table_->Count();
  for (uint32_t i = 0; i < handle->EntryNum(); i++) {
    BlockId bid = handle->GetBlockId();
    FrameId fid = block_table_[bid];
    assert(fid != INVALID_FRAME_ID);
    uint64_t data_handle = handler_->GenHandle(fid, i);

    hash_table_->Insert(Slice(handle->Read(i)->key, kKeyLength), data_handle);
  }
  LOG_ASSERT(hash_table_->Count() - count == kItemNum, "Less than expected entries inserted. expected %d, got %lu",
             kItemNum, hash_table_->Count() - count);
}
}  // namespace kv