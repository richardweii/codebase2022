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
  hash_index_ = new HashTable(kKeyNum / kPoolShardNum, &keys_);
}

Pool::~Pool() {
  // hash_index_->PrintCounter();
  delete cache_;
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
  cache_->Pin(write_line_);
  write_line_->Addr = addr.RawAddr();
  write_line_->Dirty = true;
  cache_->Release(write_line_);
}

bool Pool::Read(const Slice &key, uint32_t hash, std::string &val) {
  Addr addr;
  {  // lockfree phase
    // existence
    addr = hash_index_->Find(key, hash);
    if (addr == Addr::INVALID_ADDR) {
#ifdef STAT
      stat::read_miss.fetch_add(1);
#endif
      return false;
    }

    // cache
    // latch_.RLock();
    CacheEntry *entry = cache_->Lookup(addr);
    // latch_.RUnlock();

    if (entry != nullptr) {
#ifdef STAT
      stat::cache_hit.fetch_add(1, std::memory_order_relaxed);
#endif
      memcpy((char *)val.data(), entry->Data()->at(addr.CacheOff()), kValueLength);
      cache_->Release(entry);
      return true;
    }
  }
  {
    latch_.WLock();
    CacheEntry *entry = cache_->Lookup(addr);
    if (entry != nullptr) {
      latch_.WUnlock();

#ifdef STAT
      stat::cache_hit.fetch_add(1, std::memory_order_relaxed);
#endif
      memcpy((char *)val.data(), entry->Data()->at(addr.CacheOff()), kValueLength);
      cache_->Release(entry);
      return true;
    }

    // cache miss
    CacheEntry *victim = replacement(addr);
    latch_.WUnlock();
    memcpy((char *)val.data(), victim->Data()->at(addr.CacheOff()), kValueLength);
    cache_->Release(victim);
    return true;
  }
}

bool Pool::Write(const Slice &key, uint32_t hash, const Slice &val) {
  {  // lockfree phase
    Addr addr = hash_index_->Find(key, hash);
    if (addr != Addr::INVALID_ADDR) {
      CacheEntry *entry = cache_->Lookup(addr, true);
      if (entry != nullptr) {
#ifdef STAT
        stat::cache_hit.fetch_add(1, std::memory_order_relaxed);
#endif
        if (!entry->Dirty) {
          entry->Dirty = true;
        }
        if (memcmp(entry->Data()->at(addr.CacheOff()), val.data(), val.size()) != 0) {
          memcpy(entry->Data()->at(addr.CacheOff()), val.data(), val.size());
        }
        cache_->Release(entry);
        return true;
      }
    }
  }
  {
    latch_.WLock();
    Addr addr = hash_index_->Find(key, hash);
    if (addr == Addr::INVALID_ADDR) {
      Addr addr(cur_block_id_, cur_kv_off_);
      hash_index_->Insert(key, hash, addr);
      writeNew(key, val);
#ifdef STAT
      if (stat::insert_num == 160000001) {
        LOG_INFO("Finish insert.");
      }
#endif
      latch_.WUnlock();
      return true;
    }
    // cache
    CacheEntry *entry = cache_->Lookup(addr);
    if (entry != nullptr) {
      latch_.WUnlock();
#ifdef STAT
      stat::cache_hit.fetch_add(1, std::memory_order_relaxed);
#endif
      if (!entry->Dirty) {
        entry->Dirty = true;
      }
      if (memcmp(entry->Data()->at(addr.CacheOff()), val.data(), val.size()) != 0) {
        memcpy(entry->Data()->at(addr.CacheOff()), val.data(), val.size());
      }
      cache_->Release(entry);
      return true;
    }
    // cache miss
    CacheEntry *victim = replacement(addr);
    latch_.WUnlock();
    memcpy(victim->Data()->at(addr.CacheOff()), val.data(), val.size());
    victim->Dirty = true;
    cache_->Release(victim);
    return true;
  }
}

CacheEntry *Pool::replacement(Addr addr) {
  // miss
#ifdef STAT
  stat::replacement.fetch_add(1);
  if (stat::replacement.load(std::memory_order_relaxed) % 10000 == 0) {
    LOG_INFO("Replacement %ld", stat::replacement.load(std::memory_order_relaxed));
  }
#endif
  CacheEntry *victim = cache_->Insert(addr);
  auto batch = client_->BeginBatch();
  if (victim->Dirty) {
#ifdef STAT
    stat::dirty_write.fetch_add(1);
#endif
    auto ret = writeToRemote(victim, &batch);
    LOG_ASSERT(ret == 0, "Write cache block %d, line %d to remote failed.", Addr(victim->Addr).BlockId(),
               Addr(victim->Addr).CacheLine());
  }
  auto ret = readFromRemote(victim, addr, &batch);
  ret = batch.FinishBatch();
  LOG_ASSERT(ret == 0, "read cache block %d, line %d from remote failed.", addr.BlockId(), addr.CacheLine());
  victim->Addr = addr.RoundUp().RawAddr();
  victim->Dirty = false;
  return victim;
}

void Pool::writeNew(const Slice &key, const Slice &val) {
#ifdef STAT
  stat::insert_num.fetch_add(1);
#endif
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
    cache_->UnPin(write_line_);
    write_line_ = cache_->Insert(new_cache_line);
    cache_->Pin(write_line_);
    if (write_line_->Dirty) {
      auto batch = client_->BeginBatch();
      auto ret = writeToRemote(write_line_, &batch);
      ret = batch.FinishBatch();
      LOG_ASSERT(ret == 0, "Write cache block %d, line %d to remote failed.", Addr(write_line_->Addr).BlockId(),
                 Addr(write_line_->Addr).CacheLine());
    }
    write_line_->Addr = new_cache_line.RawAddr();
    write_line_->Dirty = true;
    cache_kv_off_ = 0;
    cache_->Release(write_line_);
  }
}

int Pool::allocNewBlock() {
#ifdef STAT
  stat::block_num.fetch_add(1);
#endif
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
  Addr ad = entry->Addr;
  BlockId bid = ad.BlockId();
  uint32_t cache_line_off = ad.CacheLine();
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