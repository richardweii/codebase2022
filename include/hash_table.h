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

class KeyBlock {
 public:
  char *GetKey(int index) {
    assert(index < kBlockValueNum);
    return &data_[index * kKeyLength];
  }

  void SetKey(int index, const Slice &key) {
    assert(index < kBlockValueNum);
    memcpy(&data_[index * kKeyLength], key.data(), key.size());
  }
  char data_[kBlockValueNum * kKeyLength];
};

class HashTable {
 public:
  HashTable(size_t size, std::vector<KeyBlock *> *keys) : keys_(keys) {
    int logn = 0;
    while (size >= 2) {
      size /= 2;
      logn++;
    }
    size_ = PrimeList[logn];
    slots_ = new HashNode[size_];
    // counter_ = new uint8_t[size_]{0};
  };

  ~HashTable() {
    HashNode *slot;
    HashNode *next;
    for (size_t i = 0; i < size_; i++) {
      if (slots_[i].next_ != nullptr) {
        slot = slots_[i].next_;
        while (slot != nullptr) {
          next = slot->next_;
          delete slot;
          slot = next;
        }
      }
    }
    delete[] slots_;
  }

  Addr Find(const Slice &key, uint32_t hash) {
    uint32_t index = hash % size_;
    uint8_t fpt = hash;
    Addr addr;
    HashNode *slot = &slots_[index];

    if (slot->addr == Addr::INVALID_ADDR) {
      return Addr::INVALID_ADDR;
    }

    while (slot != nullptr) {
      addr = slot->addr;
      if (fpt == slot->fingerprint &&
          (memcmp(key.data(), keys_->at(addr.BlockId())->GetKey(addr.BlockOff()), kKeyLength) == 0)) {
        return slot->addr;
      }
      slot = slot->next_;
    }
    return Addr::INVALID_ADDR;
  }

  void Insert(const Slice &key, uint32_t hash, Addr addr) {
    uint32_t index = hash % size_;
    uint8_t fpt = hash;
    Addr tmp_addr;

    HashNode *slot = &slots_[index];
    if (slot->addr == Addr::INVALID_ADDR) {
      slot->addr = addr;
      slot->fingerprint = fpt;
      count_++;
      // counter_[index]++;
      return;
    }

    // find
    while (slot != nullptr) {
      tmp_addr = slot->addr;
      if (fpt == slot->fingerprint &&
          (memcmp(key.data(), keys_->at(addr.BlockId())->GetKey(addr.BlockOff()), kKeyLength) == 0)) {
        // duplicate
        LOG_DEBUG("slot addr %x, addr %x", slot->addr.RawAddr(), addr.RawAddr());
        return;
      }
      slot = slot->next_;
    }

    // insert into head
    slot = new HashNode;
    slot->addr = addr;
    slot->fingerprint = fpt;
    slot->next_ = slots_[index].next_;
    slots_[index].next_ = slot;
    count_++;
    // counter_[index]++;
  }

  // void PrintCounter() {
  //   std::vector<uint32_t> count(255, 0);
  //   for (size_t i = 0; i < size_; i++) {
  //     // for (int j = 0; j <= counter_[i]; j++) {
  //     count[counter_[i]]++;
  //     // }
  //   }
  //   LOG_INFO("@@@@@@@@@@@@@@@@@@@@ Hash Table @@@@@@@@@@@@@@@@");
  //   for (int i = 0; i < 15; i++) {
  //     LOG_INFO("bucket size %d: %d", i, count[i]);
  //   }
  // }

 private:
  struct HashNode {
    uint8_t fingerprint;
    Addr addr = Addr::INVALID_ADDR;
    HashNode *next_ = nullptr;
  };

  std::vector<KeyBlock *> *keys_ = nullptr;
  HashNode *slots_ = nullptr;
  size_t count_ = 0;
  size_t size_ = 0;
  // uint8_t *counter_ = nullptr;
};

}  // namespace kv