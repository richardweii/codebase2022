#pragma once
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>
#include "config.h"
#include "util/defer.h"
#include "util/hash.h"
#include "util/logging.h"
#include "util/rwlock.h"
#include "util/slice.h"

namespace kv {

constexpr static const unsigned long PrimeList[] = {
    2ul,         3ul,         7ul,         11ul,         29lu,         53ul,        97ul,       193ul,      389ul,
    769ul,       1543ul,      3079ul,      6151ul,       12289ul,      24593ul,     49157ul,    98317ul,    196613ul,
    393241ul,    786433ul,    1572869ul,   3145739ul,    6291469ul,    12582917ul,  25165843ul, 50331653ul, 100663319ul,
    201326611ul, 402653189ul, 805306457ul, 1610612741ul, 3221225473ul, 4294967291ul};

constexpr int kSegLatchOff = 5;

class KeySlot {
 public:
  constexpr static int INVALID_SLOT_ID = -1;
  void SetKey(const Slice &s) { memcpy(_key, s.data(), kKeyLength); }
  void SetAddr(Addr addr) { _addr = addr; }
  void SetNext(int next) { _next.store(next, std::memory_order_relaxed); }

  const char *Key() const { return _key; }
  kv::Addr Addr() const { return _addr; }
  int Next() const { return _next.load(std::memory_order_relaxed); }

 private:
  friend class SlotMonitor;
  KeySlot() = default;
  // TODO: 可以把这个用一个指针指向，这样L3缓存可以缓存更多条目（感觉没什么用，访问不具备空间局部性）
  char _key[kKeyLength];
  kv::Addr _addr = INVALID_ADDR;
  std::atomic_int _next = {-1};  // next hashtable node or next free slot
};

class SlotMonitor {
 public:
  SlotMonitor();

  KeySlot *operator[](int idx) {
    LOG_ASSERT(idx >= 0, "idx %d < 0", idx);
    LOG_ASSERT((uint32_t)idx < kKeyNum / kPoolShardingNum, "idx %d out of range %lu", idx, kKeyNum / kPoolShardingNum);
    return &_slots[idx];
  }
  int GetNewSlot(KeySlot **out);
  void FreeSlot(KeySlot *slot);

 private:
  // TODO: lock-free list
  SpinLock _lock;
  int _free_slot_head = -1;
  KeySlot _slots[kKeyNum / kPoolShardingNum];
};

class HashTable {
 public:
  HashTable(size_t size);

  ~HashTable() { delete[] _bucket; }

  KeySlot *Find(const Slice &key, uint32_t hash);

  // DO NOT check duplicate
  KeySlot *Insert(const Slice &key, uint32_t hash);

  // need free the slot to SlotMonitor
  KeySlot *Remove(const Slice &key, uint32_t hash);

  SlotMonitor *GetSlotMonitor() { return &_monitor; }

 private:
  SlotMonitor _monitor;
  int *_bucket = nullptr;
  size_t _size = 0;
  SpinLatch _seg_latch[(kKeyNum / kPoolShardingNum) >> kSegLatchOff];
};

}  // namespace kv