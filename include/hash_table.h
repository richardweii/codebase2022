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
  char _key[kKeyLength];
  kv::Addr _addr = INVALID_ADDR;
  std::atomic_int _next = {-1};  // next hashtable node or next free slot
};

class HashTable {
 public:
  HashTable(size_t size, KeySlot *slots) : _slots(slots) {
    int logn = 0;
    while (size >= 2) {
      size /= 2;
      logn++;
    }
    _size = PrimeList[logn];
    _bucket = new std::atomic_int[_size];

    // init bucket
    for (size_t i = 0; i < _size; i++) {
      _bucket[i].store(-1, std::memory_order_relaxed);
    }
    counter_ = new uint8_t[_size]{0};
  };

  ~HashTable() {
    // PrintCounter();
    delete[] _bucket;
  }

  KeySlot* Find(const Slice &key, uint32_t hash) {
    hash >>= kPoolShardingBits;
    uint32_t index = hash % _size;
    int slot_id = _bucket[index];

    KeySlot *slot = nullptr;
    while (slot_id != KeySlot::INVALID_SLOT_ID) {
      slot = &_slots[slot_id];
      if ((memcmp(key.data(), slot->Key(), kKeyLength) == 0)) {
        return slot;
      }
      slot_id = slot->Next();
    }
    return nullptr;
  }

  // DO NOT check duplicate
  bool Insert(const Slice &key, uint32_t hash, int new_slot_id) {
    hash >>= kPoolShardingBits;
    uint32_t index = hash % _size;

    int slot_id = _bucket[index];
    if (slot_id == KeySlot::INVALID_SLOT_ID) {
      _bucket[index].store(new_slot_id, std::memory_order_relaxed);
      _count++;
      counter_[index]++;
      return true;
    }

    KeySlot *slot = nullptr;
#ifdef TEST_CONFIG
    // find
    while (slot_id != KeySlot::INVALID_SLOT_ID) {
      slot = &_slots[slot_id];
      if ((memcmp(key.data(), slot->Key(), kKeyLength) == 0)) {
        LOG_DEBUG("duplicate of slot %d", new_slot_id);
        return false;
      }
      slot_id = slot->Next();
    }
#endif

    // insert into head
    slot = &_slots[new_slot_id];
    slot->SetNext(_bucket[index].load(std::memory_order_relaxed));

    _bucket[index].store(new_slot_id, std::memory_order_relaxed);
    _count++;
    counter_[index]++;
    return true;
  }

  int Remove(const Slice &key, uint32_t hash) {
    hash >>= kPoolShardingBits;
    uint32_t index = hash % _size;

    int slot_id = _bucket[index];
    if (slot_id == KeySlot::INVALID_SLOT_ID) {
      return KeySlot::INVALID_SLOT_ID;
    }

    // head
    KeySlot *slot = &_slots[slot_id];
    if (memcmp(slot->Key(), key.data(), kKeyLength) == 0) {
      _bucket[index].store(slot->Next(), std::memory_order_relaxed);
      counter_[index]--;
      return slot_id;
    }

    // find
    int front_slot_id = slot_id;
    int cur_slot_id = slot->Next();
    while (cur_slot_id != KeySlot::INVALID_SLOT_ID) {
      slot = &_slots[cur_slot_id];
      if (memcmp(slot->Key(), key.data(), kKeyLength) == 0) {
        _slots[front_slot_id].SetNext(slot->Next());
        counter_[index]--;
        return cur_slot_id;
      }
      front_slot_id = cur_slot_id;
      cur_slot_id = slot->Next();
    }
    // cannot find
    return KeySlot::INVALID_SLOT_ID;
  }

  void PrintCounter() {
    std::vector<uint32_t> count(255, 0);
    for (size_t i = 0; i < _size; i++) {
      count[counter_[i]]++;
    }
    LOG_INFO("@@@@@@@@@@@@@@@@@@@@ Hash Table @@@@@@@@@@@@@@@@");
    for (int i = 0; i < 15; i++) {
      LOG_INFO("bucket size %d: %d", i, count[i]);
    }
  }

 private:
  KeySlot *_slots = nullptr;
  std::atomic_int *_bucket = nullptr;
  size_t _count = 0;
  size_t _size = 0;
  uint8_t *counter_ = nullptr;
};

}  // namespace kv