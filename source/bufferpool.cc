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

static std::atomic<uint32_t> rid_counter(1);

uint32_t getRid() { return rid_counter.fetch_add(1, std::memory_order_relaxed); }

BufferPool::BufferPool(size_t size, uint8_t shard, RDMAClient *client) {
  shard_ = shard;
  pool_size_ = size;
  datablocks_ = new DataBlock[size];
  handles_.reserve(size);
  for (size_t i = 0; i < size; i++) {
    handles_.emplace_back(new BlockHandle(&datablocks_[i]));
  }
  filter_ = NewBloomFilterPolicy();
  client_ = client;
  pd_ = client->Pd();
  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
  clock_frame_ = new std::atomic_bool[size];
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

    // erase old
    WriteLockTable();
    ret = block_table_.erase(datablocks_[frame_id].GetId());
    assert(ret == 1);
    WriteUnlockTable();

    ret = client_->RemoteWrite(&datablocks_[frame_id], mr_->lkey, kDataBlockSize, addr, rkey);
    if (alloced) {
      createRemoteIndex(shard_, old_bid);
    }

    LOG_ASSERT(ret == 0, "Remote Write Datablock Failed.");
    LOG_DEBUG("shard %d, Write datablock %d to remote successfully...", shard_, old_bid);
  }

  BlockId id = genBlockId();
  datablocks_[frame_id].Free();
  datablocks_[frame_id].SetId(id);

  // insert new
  WriteLockTable();
  block_table_[datablocks_[frame_id].GetId()] = frame_id;
  WriteUnlockTable();

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

void BufferPool::createRemoteIndex(uint8_t shard, BlockId id) {
  CreateIndexRequest req;
  req.shard = shard;
  req.id = id;
  req.rid = getRid();
  req.type = MSG_CREATE;

  client_->Async(req);
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

  ret = client_->RemoteWrite(&datablocks_[frame_id], mr_->lkey, kDataBlockSize, write_addr, write_rkey);
  LOG_ASSERT(ret == 0, "Remote Write Datablock Failed.");
  if (alloced) {
    createRemoteIndex(shard_, victim);
  }
  // erase old
  WriteLockTable();
  ret = block_table_.erase(datablocks_[frame_id].GetId());
  datablocks_[frame_id].Free();
  assert(ret == 1);
  WriteUnlockTable();

  // fetch
  ret = client_->RemoteRead(&datablocks_[frame_id], mr_->lkey, kDataBlockSize, read_addr, read_rkey);
  LOG_ASSERT(ret == 0, "Remote Read Datablock Failed.");
  LOG_DEBUG("Read Block %d", datablocks_[frame_id].GetId());

  // insert new
  WriteLockTable();
  block_table_[datablocks_[frame_id].GetId()] = frame_id;
  WriteUnlockTable();

  // LRU update
  renew(frame_id);

  LOG_DEBUG("Replacement finish.");

  fid = frame_id;
  return found;
}

void BufferPool::renew(FrameId frame_id) { clock_frame_[frame_id].store(true); }

int BufferPool::walk() {
  int ret = hand_;
  hand_ = (hand_ + 1) % pool_size_;
  return ret;
}

FrameId BufferPool::pop() {
  bool found = false;
  while (!found) {
    bool ref = true;
    if (clock_frame_[hand_].compare_exchange_weak(ref, false)) {
      walk();
      continue;
    } else {
      found = true;
    }
  }
  return walk();
}

bool BufferPool::FetchRead(Slice key, std::string &value, CacheEntry &entry) {
  for (auto &kv : block_table_) {
    FrameId fid = kv.second;

    BlockHandle *handle = handles_[fid];
    if (handle->Read(key, value, filter_, entry)) {
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

  bool succ = handles_[fid]->Read(key, value, filter_, entry);
  LOG_ASSERT(succ, "Fetched invalid datablock.")
  return true;
}

bool BufferPool::MissFetch(Slice key, BlockId id) {
  LOG_DEBUG("Fetch block %d for key %s", id, key.data());

  if (block_table_.count(id)) {
    LOG_DEBUG("Other has fetched the block.");
    return true;
  }

  stat::fetch.fetch_add(1, std::memory_order_relaxed);
  int ret;
  bool succ;
  // replacement
  FrameId frame_id;
  frame_id = pop();
  if (frame_id == INVALID_FRAME_ID) {
    return false;
  }

  BlockId victim = datablocks_[frame_id].GetId();

  // write back victim
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
  ret = client_->RemoteWrite(&datablocks_[frame_id], mr_->lkey, kDataBlockSize, write_addr, write_rkey);
  if (alloced) {
    createRemoteIndex(shard_, victim);
  }
  LOG_ASSERT(ret == 0, "Remote Write Datablock Failed.");

  // erase old
  WriteLockTable();
  ret = block_table_.erase(victim);
  datablocks_[frame_id].Free();
  assert(ret == 1);
  WriteUnlockTable();

  // fetch
  uint64_t read_addr = global_table_[id].addr;
  uint32_t read_rkey = global_table_[id].rkey;
  // fetch
  ret = client_->RemoteRead(&datablocks_[frame_id], mr_->lkey, kDataBlockSize, read_addr, read_rkey);
  LOG_ASSERT(ret == 0, "Remote Read Datablock Failed.");
  LOG_DEBUG("Read Block %d", id);
  LOG_ASSERT(datablocks_[frame_id].GetId() == id, "Unmatched block id.");
  // insert new
  WriteLockTable();
  block_table_[id] = frame_id;
  WriteUnlockTable();

  // LRU update
  renew(frame_id);

  LOG_DEBUG("Replacement finish.");

  LOG_ASSERT(handles_[frame_id]->Find(key, this->filter_), "Invalid key in cache.");
  LOG_ASSERT(HasBlock(id), "Failed to fetch block %d", id);
  return true;
}

bool BufferPool::Read(Slice key, std::string &value, CacheEntry &entry) {
  for (auto &kv : block_table_) {
    FrameId fid = kv.second;

    BlockHandle *handle = handles_[fid];
    if (handle->Read(key, value, filter_, entry)) {
      renew(fid);
      stat::local_access.fetch_add(1);
      return true;
    }
  }
  return false;
}

bool BufferPool::Modify(Slice key, Slice value, CacheEntry &entry) {
  for (auto &kv : block_table_) {
    BlockId id = kv.first;
    FrameId fid = kv.second;

    BlockHandle *handle = handles_[fid];
    if (handle->Modify(key, value, filter_, entry)) {
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

  bool succ = handles_[fid]->Modify(key, value, filter_, entry);
  LOG_ASSERT(succ, "Fetched invalid datablock.")
  return true;
}
}  // namespace kv