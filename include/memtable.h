#pragma once

#include <cassert>
#include <cstddef>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "block.h"
#include "config.h"
#include "util/hash.h"
#include "util/logging.h"
#include "util/nocopy.h"

class MemTable NOCOPYABLE {
  struct InternalKeyHash {
    size_t operator()(const InternalKey &key) const { return Hash(key->c_str(), key->size(), 0x1237423); }
  };

  struct InternalKeyEqual {
    bool operator()(const InternalKey &a, const InternalKey &b) const { return *a == *b; }
  };

 public:
  MemTable(int cap = kItemNum) : cap_(cap) {}

  bool insert(InternalKey key, InternalValue value) {
    LOG_ASSERT(count_ < cap_, "Insert too many items to memtable, need build a block.");
    if (!table_.count(key)) {
      count_++;
    }
    table_[key] = value;
    return true;
  }

  InternalValue read(InternalKey key) {
    if (table_.count(key)) {
      return table_[key];
    }
    return nullptr;
  }

  Ptr<DataBlock> buildDataBlock();

 private:
  std::unordered_map<InternalKey, InternalValue, InternalKeyHash, InternalKeyEqual> table_;
  int count_ = 0;
  int cap_;
};