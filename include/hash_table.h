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
  struct Entry {
    char key[kKeyLength];
    ID id = Identifier::INVALID_ID;
    ID next_ = Identifier::INVALID_ID;
  };

  Entry *GetEnt(ID id) {
    int index = Identifier::BlockOff(id);
    assert(index < kBlockValueNum);
    return &data_[index];
  }

  void SetKey(ID id, const Slice &key) {
    int index = Identifier::BlockOff(id);
    assert(index < kBlockValueNum);
    data_[index].id = id;
    memcpy(data_[index].key, key.data(), key.size());
  }
  Entry data_[kBlockValueNum];
};

class HashTable {
 public:
  HashTable(size_t size, std::vector<KeyBlock *> *ents) : ents_(ents) {
    int logn = 0;
    while (size >= 2) {
      size /= 2;
      logn++;
    }
    size_ = PrimeList[logn + 1];
    slots_ = new ID[size_]{Identifier::INVALID_ID};
    memset(slots_, 0xff, size_ * sizeof(ID));
    // counter_ = new uint8_t[size_]{};
  };

  ~HashTable() { delete[] slots_; }

  ID Find(const Slice &key, uint32_t hash) {
    uint32_t index = hash % size_;
    ID id = slots_[index];

    while (id != Identifier::INVALID_ID) {
      auto *ent = ents_->at(Identifier::GetBlockId(id))->GetEnt(id);
      if (memcmp(key.data(), ent->key, kKeyLength) == 0) {
        return id;
      }
      id = ent->next_;
    }
    return Identifier::INVALID_ID;
  }

  void Insert(const Slice &key, uint32_t hash, ID id) {
    uint32_t index = hash % size_;
    auto *new_ent = ents_->at(Identifier::GetBlockId(id))->GetEnt(id);

    ID tmp = slots_[index];
    if (tmp == Identifier::INVALID_ID) {
      slots_[index] = id;
      count_++;
      // counter_[index]++;
      return;
    }

    // find
    while (tmp != Identifier::INVALID_ID) {
      auto *ent = ents_->at(Identifier::GetBlockId(tmp))->GetEnt(tmp);
      if (memcmp(key.data(), ent->key, kKeyLength) == 0) {
        // duplicate
        LOG_DEBUG("slot id %x, id %x", ent->id, id);
        return;
      }
      tmp = ent->next_;
    }

    // insert into head
    new_ent->next_ = slots_[index];
    slots_[index] = id;
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
  std::vector<KeyBlock *> *ents_ = nullptr;
  ID *slots_ = nullptr;
  size_t count_ = 0;
  size_t size_ = 0;
  // uint8_t *counter_ = nullptr;
};

}  // namespace kv