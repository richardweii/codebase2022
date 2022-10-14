#pragma once
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <queue>
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
constexpr int kSegLatchMask = (kKeyNum / kPoolShardingNum) >> kSegLatchOff;
// constexpr int kSegLatchMask = (1UL << kSegLatchOff) - 1;

class KeySlot {
 public:
  constexpr static int INVALID_SLOT_ID = -1;
  void SetKey(const Slice &s) { memcpy(_key, s.data(), kKeyLength); }
  void SetAddr(Addr addr) { _addr = addr; }
  void SetNext(int next) { _next = next; }

  const char *Key() const { return _key; }
  kv::Addr Addr() const { return _addr; }
  int Next() const { return _next; }

 private:
  friend class SlotMonitor;
  KeySlot() = default;
  char _key[kKeyLength];
  kv::Addr _addr = INVALID_ADDR;
  int _next = -1;  // next hashtable node or next free slot
};

class Queue {
 private:
  int size;
  int *arr;
  int head;
  int tail;
  int mask;

 public:
  Queue(int size) {
    head = 0;
    tail = 0;
    this->size = roundup_power_of_2(size);
    arr = new int[this->size];
    mask = this->size - 1;
  }
  bool isFull() { return (tail + 1) % size == head; }
  bool isEmpty() { return head == tail; }
  void enqueue(int val) {
    arr[tail] = val;
    tail = (tail + 1) & mask;
  }
  int dequeue() {
    int val = arr[head];
    head = (head + 1) & mask;
    return val;
  }
  uint32_t roundup_power_of_2(uint32_t val) {
    // 已经是2的幂了，可直接返回
    if ((val & (val - 1)) == 0) {
      return val;
    }

    // uint32类型中，2的次幂最大数
    uint32_t andv = 0x80000000;

    // 逐位右移，直到找到满足val第一个位为1的
    while ((andv & val) == 0) {
      andv = andv >> 1;
    }

    // 再左移一位，确保比val大
    return andv << 1;
  }
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
  int GetSlotIdx(KeySlot *slot) { return static_cast<int>(slot - _slots); }

 private:
  Queue *_free_slots[kThreadNum];
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
};

}  // namespace kv