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

  uint64_t Handle() const { return data_handle_; }

 private:
  HashNode *next_ = nullptr;
  uint64_t data_handle_ = INVALID_HANDLE;
};

template <typename Tp>
class HashTable {
 public:
  static uint32_t Hash(Tp key);

  HashTable(size_t size, HashHandler<Tp> *handler, bool latch = false) : handler_(handler), size_(size), latch_(latch) {
    int logn = 0;
    while (size >= 2) {
      size /= 2;
      logn++;
    }
    size_ = PrimeList[logn];
    slots_ = new HashNode<Tp>[size_];
    if (latch_) {
      slot_latch_ = new SpinLatch[size_];
    }
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

  HashNode<Tp> *Find(const Tp &key) {
    uint32_t index = Hash(key) % size_;
    HashNode<Tp> *slot = &slots_[index];
    if (latch_) rlock(index);
    if (slot->data_handle_ == INVALID_HANDLE) {
      if (latch_) rUnlock(index);
      return nullptr;
    }

    while (slot != nullptr) {
      if (key == handler_->GetKey(slot->data_handle_)) {
        if (latch_) rUnlock(index);
        return slot;
      }
      slot = slot->next_;
    }
    if (latch_) rUnlock(index);
    return nullptr;
  }

  void Insert(const Tp &key, uint64_t data_handle) {
    uint32_t index = Hash(key) % size_;
    HashNode<Tp> *slot = &slots_[index];
    if (latch_) wlock(index);
    if (slot->data_handle_ == INVALID_HANDLE) {
      slot->data_handle_ = data_handle;
      count_++;
      if (latch_) wUnlock(index);
      return;
    }

    // find
    while (slot != nullptr) {
      if (key == handler_->GetKey(slot->data_handle_)) {
        // duplicate
        LOG_DEBUG("slot data_handle %lx, data_handle %lx", slot->data_handle_, data_handle);
        if (latch_) wUnlock(index);
        return;
      }
      slot = slot->next_;
    }

    // insert into head
    slot = slots_[index].next_;
    slots_[index].next_ = new HashNode<Tp>(data_handle);
    slots_[index].next_->next_ = slot;
    count_++;
    if (latch_) wUnlock(index);
  }

  bool Remove(const Tp &key) {
    uint32_t index = Hash(key) % size_;
    HashNode<Tp> *slot = &slots_[index];
    if (latch_) wlock(index);

    if (slot->data_handle_ == INVALID_HANDLE) {
      if (latch_) wUnlock(index);
      return false;
    }

    // head
    if (key == handler_->GetKey(slot->data_handle_)) {
      if (slot->next_ != nullptr) {
        HashNode<Tp> *tmp = slot->next_;
        *slot = *slot->next_;
        delete tmp;
      } else {
        slot->data_handle_ = INVALID_HANDLE;
        slot->next_ = nullptr;
      }
      count_--;
      if (latch_) wUnlock(index);
      return true;
    }

    // find
    HashNode<Tp> *front = slot;
    while (slot != nullptr) {
      if (key == handler_->GetKey(slot->data_handle_)) {
        front->next_ = slot->next_;
        delete slot;
        count_--;
        if (latch_) wUnlock(index);
        return true;
      }
      front = slot;
      slot = slot->next_;
    }
    // cannot find
    if (latch_) wUnlock(index);
    return false;
  }

  size_t SlotSize() const { return size_; }
  size_t Count() const { return count_; }

 private:
  void rlock(int index) { slot_latch_[index].RLock(); }
  void rUnlock(int index) { slot_latch_[index].RUnlock(); }
  void wlock(int index) { slot_latch_[index].WLock(); }
  void wUnlock(int index) { slot_latch_[index].WUnlock(); }

  constexpr static uint32_t hash_seed_ = 0xf6ec23d9;
  HashHandler<Tp> *handler_;
  HashNode<Tp> *slots_ = nullptr;
  SpinLatch *slot_latch_ = nullptr;
  size_t count_ = 0;
  size_t size_ = 0;
  bool latch_ = false;
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