#pragma once
#include <cassert>
#include <cstddef>
#include <cstdint>
#include "config.h"
#include "util/hash.h"
#include "util/slice.h"

namespace kv {

constexpr static const unsigned long PrimeList[] = {
    2ul,         3ul,         7ul,         11ul,         29lu,         53ul,        97ul,       193ul,      389ul,
    769ul,       1543ul,      3079ul,      6151ul,       12289ul,      24593ul,     49157ul,    98317ul,    196613ul,
    393241ul,    786433ul,    1572869ul,   3145739ul,    6291469ul,    12582917ul,  25165843ul, 50331653ul, 100663319ul,
    201326611ul, 402653189ul, 805306457ul, 1610612741ul, 3221225473ul, 4294967291ul};

constexpr static uint64_t INVALID_HANDLE = ~0;

class HashHandler {
 public:
  virtual ~HashHandler(){};
  virtual Slice GetKey(uint64_t data_handle) = 0;
};

class HashNode {
 public:
  friend class HashTable;
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

class HashTable {
 public:
  static uint32_t Hash(Slice key) { return kv::Hash(key.data(), key.size(), hash_seed_); }

  HashTable(size_t size, HashHandler *handler) : handler_(handler), size_(size) {
    int logn = 0;
    while (size >= 2) {
      size /= 2;
      logn++;
    }
    size_ = PrimeList[logn];
    slots_ = new HashNode[size_];
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

  HashNode *Find(Slice key) {
    uint32_t index = Hash(key) % size_;
    HashNode *slot = &slots_[index];
    if (!slot->IsValid()) {
      return nullptr;
    }

    while (slot != nullptr) {
      if (key == handler_->GetKey(slot->data_handle_)) {
        return slot;
      }
      slot = slot->next_;
    }
    return nullptr;
  }

  void Insert(Slice key, uint64_t data_handle) {
    uint32_t index = Hash(key) % size_;
    HashNode *slot = &slots_[index];

    if (!slot->IsValid()) {
      slot->data_handle_ = data_handle;
      count_++;
      return;
    }

    // find
    while (slot != nullptr) {
      if (key == handler_->GetKey(slot->data_handle_)) {
        // duplicate
        return;
      }
      slot = slot->next_;
    }

    // insert into head
    slot = slots_[index].next_;
    slots_[index].next_ = new HashNode(data_handle);
    slots_[index].next_->next_ = slot;
    count_++;
  }

  bool Remove(Slice key) {
    uint32_t index = Hash(key) % size_;
    HashNode *slot = &slots_[index];

    if (!slot->IsValid()) {
      return false;
    }

    // head
    if (key == handler_->GetKey(slot->data_handle_)) {
      if (slot->next_ != nullptr) {
        HashNode *tmp = slot->next_;
        *slot = *slot->next_;
        delete tmp;
      } else {
        slot->data_handle_ = INVALID_HANDLE;
        slot->next_ = nullptr;
      }
      count_--;
      return true;
    }

    // find
    HashNode *front = slot;
    while (slot != nullptr) {
      if (key == handler_->GetKey(slot->data_handle_)) {
        front->next_ = slot->next_;
        delete slot;
        count_--;
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

 private:
  constexpr static uint32_t hash_seed_ = 0xf6ec23d9;
  HashHandler *handler_;
  HashNode *slots_ = nullptr;
  size_t count_ = 0;
  size_t size_ = 0;
};
}  // namespace kv