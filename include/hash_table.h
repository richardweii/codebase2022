#pragma once
#include <cassert>
#include <cstddef>
#include <cstdint>
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

constexpr static uint64_t INVALID_HANDLE = ~0;
constexpr static uint32_t FINGERPRINT_MASK = 0x3ff;
constexpr static uint64_t DATA_HANDLE_MASK = ~0xffc0000000000000;

template <typename Tp>
class HashHandler {
 public:
  virtual ~HashHandler(){};
  virtual Tp GetKey(uint64_t data_handle) = 0;
};

template <typename Tp>
class HashTable;

template <typename Tp>
class HashNode {
 public:
  friend class HashTable<Tp>;
  HashNode() = default;
  HashNode(uint64_t data_handle) : data_handle_(data_handle){};

  HashNode *Next() const { return next_; }
  void SetNext(HashNode *next) { next_ = next; }

  bool IsValid() const { return data_handle_ != INVALID_HANDLE; }

  uint64_t Handle() const { return data_handle_ & DATA_HANDLE_MASK; }

  uint32_t Fingerprint() const { return data_handle_ >> 54; }

 private:
  HashNode *next_ = nullptr;
  uint64_t data_handle_ = INVALID_HANDLE;
};

template <typename Tp>
class HashTable {
 public:
  static uint32_t Hash(Tp key);

  HashTable(size_t size, HashHandler<Tp> *handler) : handler_(handler), size_(size) {
    int logn = 0;
    while (size >= 2) {
      size /= 2;
      logn++;
    }
    size_ = PrimeList[logn];
    slots_ = new HashNode<Tp>[size_];
    // counter_ = new uint8_t[size_]{0};
  };

  ~HashTable() {
    HashNode<Tp> *slot;
    HashNode<Tp> *next;
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

  HashNode<Tp> *Find(const Tp &key, uint32_t hash) {
    uint32_t index = hash % size_;
    uint32_t fpt = hash & FINGERPRINT_MASK;

    HashNode<Tp> *slot = &slots_[index];
    if (slot->data_handle_ == INVALID_HANDLE) {
      return nullptr;
    }

    while (slot != nullptr) {
      if (fpt == slot->Fingerprint() && key == handler_->GetKey(slot->Handle())) {
        return slot;
      }
      slot = slot->next_;
    }
    return nullptr;
  }

  void Insert(const Tp &key, uint32_t hash, uint64_t data_handle) {
    uint32_t index = hash % size_;
    uint32_t fpt = hash & FINGERPRINT_MASK;

    data_handle |= ((uint64_t)fpt << 54);

    HashNode<Tp> *slot = &slots_[index];
    if (slot->data_handle_ == INVALID_HANDLE) {
      slot->data_handle_ = data_handle;
      count_++;
      // counter_[index]++;
      return;
    }

    // find
    while (slot != nullptr) {
      if (fpt == slot->Fingerprint() && key == handler_->GetKey(slot->Handle())) {
        // duplicate
        LOG_DEBUG("slot data_handle %lx, data_handle %lx", slot->data_handle_, data_handle);
        return;
      }
      slot = slot->next_;
    }

    // insert into head
    slot = slots_[index].next_;
    slots_[index].next_ = new HashNode<Tp>(data_handle);
    slots_[index].next_->next_ = slot;
    count_++;
    // counter_[index]++;
  }

  bool Remove(const Tp &key, uint32_t hash) {
    uint32_t index = hash % size_;
    uint32_t fpt = hash & FINGERPRINT_MASK;

    HashNode<Tp> *slot = &slots_[index];

    if (slot->data_handle_ == INVALID_HANDLE) {
      return false;
    }

    // head
    if (fpt == slot->Fingerprint() && key == handler_->GetKey(slot->Handle())) {
      if (slot->next_ != nullptr) {
        HashNode<Tp> *tmp = slot->next_;
        *slot = *slot->next_;
        delete tmp;
      } else {
        slot->data_handle_ = INVALID_HANDLE;
        slot->next_ = nullptr;
      }
      count_--;
      // counter_[index]--;
      return true;
    }

    // find
    HashNode<Tp> *front = slot;
    while (slot != nullptr) {
      if (fpt == slot->Fingerprint() && key == handler_->GetKey(slot->Handle())) {
        front->next_ = slot->next_;
        delete slot;
        count_--;
        // counter_[index]--;
        return true;
      }
      front = slot;
      slot = slot->next_;
    }
    // cannot find
    return false;
  }

  size_t SlotSize() const { return size_; }
  size_t Count() const { return count_; }

  // void PrintCounter() {
  //   std::vector<uint32_t> count(255, 0);
  //   for (size_t i = 0; i < size_; i++) {
  //     for (int j = 0; j <= counter_[i]; j++) {
  //       count[j]++;
  //     }
  //   }
  //   LOG_INFO("@@@@@@@@@@@@@@@@@@@@ Hash Table @@@@@@@@@@@@@@@@");
  //   for (int i = 0; i < 15; i++) {
  //     LOG_INFO("bucket size %d: %d", i, count[i]);
  //   }
  // }

 private:
  constexpr static uint32_t hash_seed_ = kPoolHashSeed;
  HashHandler<Tp> *handler_;
  HashNode<Tp> *slots_ = nullptr;
  size_t count_ = 0;
  size_t size_ = 0;
  bool latch_ = false;
  // uint8_t *counter_ = nullptr;
};

template <>
inline uint32_t HashTable<Slice>::Hash(Slice key) {
  return kv::Hash(key.data(), key.size(), hash_seed_);
}

template <>
inline uint32_t HashTable<uint32_t>::Hash(uint32_t key) {
  return key;
}

template <>
inline uint32_t HashTable<uint64_t>::Hash(uint64_t key) {
  return key;
}
}  // namespace kv