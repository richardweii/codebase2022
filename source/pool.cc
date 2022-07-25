#include "pool.h"
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <thread>
#include "cache.h"
#include "config.h"
#include "hash_table.h"
#include "msg.h"
#include "rdma_manager.h"
#include "stat.h"
#include "util/defer.h"
#include "util/logging.h"

namespace kv {

Pool::Pool(size_t cache_size, uint8_t shard, RDMAClient *client) : client_(client), shard_(shard) {
  cache_ = new Cache(cache_size);
  handler_ = new PoolHashHandler(&keys_);
  hash_index_ = new HashTable<Slice>(kKeyNum / kPoolShardNum, handler_);
}

Pool::~Pool() {
  delete cache_;
  delete handler_;
  delete hash_index_;
  for (size_t i = 0; i < keys_.size(); i++) {
    delete keys_[i];
  }
}

void Pool::Init() {
  auto succ = cache_->Init(client_->Pd());
  LOG_ASSERT(succ, "Init cache failed.");
  auto ret = allocNewBlock();
  LOG_ASSERT(ret == 0, "Alloc first block failed.");
  Addr addr(cur_block_id_, cur_kv_off_);
  write_line_ = cache_->Insert(addr);
  write_line_->Addr = addr;
  write_line_->Dirty = true;
}

bool Pool::Read(const Slice &key, std::string &val) {
  Addr addr;
  {
    // existence
    latch_.RLock();
    defer { latch_.RUnlock(); };
    auto node = hash_index_->Find(key);
    if (node == nullptr) {
      stat::read_miss.fetch_add(1);
      return false;
    }
    addr = handler_->GetAddr(node->Handle());

    val.resize(kValueLength);

    // cache
    CacheEntry *entry = cache_->Lookup(addr);
    if (entry != nullptr) {
      stat::cache_hit.fetch_add(1);
      memcpy((char *)val.data(), entry->Data()->at(addr.CacheOff()), kValueLength);
      cache_->Release(entry);
      return true;
    }
  }
  {
    latch_.WLock();
    defer { latch_.WUnlock(); };
    CacheEntry *entry = cache_->Lookup(addr);
    if (entry != nullptr) {
      stat::cache_hit.fetch_add(1);
      memcpy((char *)val.data(), entry->Data()->at(addr.CacheOff()), kValueLength);
      cache_->Release(entry);
      return true;
    }

    // cache miss
    CacheEntry *victim = replacement(addr);
    memcpy((char *)val.data(), victim->Data()->at(addr.CacheOff()), kValueLength);
    cache_->Release(victim);
    return true;
  }
}

bool Pool::Write(const Slice &key, const Slice &val) {
  {
    // for cache
    latch_.RLock();
    defer { latch_.RUnlock(); };
    auto node = hash_index_->Find(key);
    if (node != nullptr) {
      Addr addr = handler_->GetAddr(node->Handle());
      // cache
      CacheEntry *entry = cache_->Lookup(addr);
      if (entry != nullptr) {
        stat::cache_hit.fetch_add(1);
        memcpy(entry->Data()->at(addr.CacheOff()), val.data(), val.size());
        entry->Dirty = true;
        cache_->Release(entry);
        return true;
      }
    }
  }
  {
    latch_.WLock();
    defer { latch_.WUnlock(); };
    auto node = hash_index_->Find(key);
    if (node == nullptr) {
      Addr addr(cur_block_id_, cur_kv_off_);
      hash_index_->Insert(key, handler_->GenHandle(addr));
      writeNew(key, val);
      if (stat::insert_num == 160000001) {
        LOG_INFO("Finish insert.");
      }
      return true;
    }

    Addr addr = handler_->GetAddr(node->Handle());

    // cache
    CacheEntry *entry = cache_->Lookup(addr);
    if (entry != nullptr) {
      stat::cache_hit.fetch_add(1);
      memcpy(entry->Data()->at(addr.CacheOff()), val.data(), val.size());
      entry->Dirty = true;
      cache_->Release(entry);
      return true;
    }
    // cache miss
    CacheEntry *victim = replacement(addr);
    memcpy(victim->Data()->at(addr.CacheOff()), val.data(), val.size());
    victim->Dirty = true;
    cache_->Release(victim);
    return true;
  }
}

CacheEntry *Pool::replacement(Addr addr) {
  // miss
  stat::replacement.fetch_add(1);
  CacheEntry *victim = cache_->Insert(addr);
  auto batch = client_->BeginBatch();
  if (victim->Dirty) {
    stat::dirty_write.fetch_add(1);
    auto ret = writeToRemote(victim, &batch);
    LOG_ASSERT(ret == 0, "Write cache block %d, line %d to remote failed.", victim->Addr.BlockId(),
               victim->Addr.CacheLine());
  }
  auto ret = readFromRemote(victim, addr, &batch);
  ret = batch.FinishBatch();
  LOG_ASSERT(ret == 0, "read cache block %d, line %d from remote failed.", addr.BlockId(), addr.CacheLine());
  victim->Addr = addr.RoundUp();
  victim->Dirty = false;
  return victim;
}

void Pool::writeNew(const Slice &key, const Slice &val) {
  stat::insert_num.fetch_add(1);
  keys_[cur_block_id_]->SetKey(cur_kv_off_, key);
  memcpy(write_line_->Data()->at(cache_kv_off_), val.data(), val.size());
  cache_kv_off_++;
  cur_kv_off_++;

  if (cur_kv_off_ == kBlockValueNum) {
    LOG_DEBUG("Need alloc new block.");
    cur_block_id_++;
    cur_kv_off_ = 0;
    auto ret = allocNewBlock();
    assert(ret == 0);
  }

  if (cache_kv_off_ == kCacheValueNum) {
    Addr new_cache_line(cur_block_id_, cur_kv_off_);
    cache_->Release(write_line_);
    write_line_ = cache_->Insert(new_cache_line);
    if (write_line_->Dirty) {
      auto batch = client_->BeginBatch();
      auto ret = writeToRemote(write_line_, &batch);
      ret = batch.FinishBatch();
      LOG_ASSERT(ret == 0, "Write cache block %d, line %d to remote failed.", write_line_->Addr.BlockId(),
                 write_line_->Addr.CacheLine());
    }
    write_line_->Addr = new_cache_line;
    write_line_->Dirty = true;
    cache_kv_off_ = 0;
  }
}

int Pool::allocNewBlock() {
  stat::block_num.fetch_add(1);
  keys_.emplace_back(new KeyBlock());
  AllocRequest req;
  req.shard = shard_;
  req.size = kValueBlockSize;
  req.type = MSG_ALLOC;

  AllocResponse resp;
  client_->RPC(req, resp);
  if (resp.status != RES_OK) {
    LOG_ERROR("Failed to alloc new block.");
    return -1;
  }
  global_addr_table_[cur_block_id_].addr = resp.addr;
  global_addr_table_[cur_block_id_].rkey = resp.rkey;
  cur_block_.addr = resp.addr;
  cur_block_.rkey = resp.rkey;
  return 0;
}

int Pool::writeToRemote(CacheEntry *entry, RDMAManager::Batch *batch) {
  BlockId bid = entry->Addr.BlockId();
  uint32_t cache_line_off = entry->Addr.CacheLine();
  MemoryAccess &access = global_addr_table_.at(bid);
  return batch->RemoteWrite(entry->Data(), cache_->MR()->lkey, sizeof(CacheLine),
                            access.addr + sizeof(CacheLine) * cache_line_off, access.rkey);
}

int Pool::readFromRemote(CacheEntry *entry, Addr addr, RDMAManager::Batch *batch) {
  BlockId bid = addr.BlockId();
  uint32_t cache_line_off = addr.CacheLine();
  MemoryAccess &access = global_addr_table_.at(bid);
  return batch->RemoteRead(entry->Data(), cache_->MR()->lkey, sizeof(CacheLine),
                           access.addr + sizeof(CacheLine) * cache_line_off, access.rkey);
}

}  // namespace kv