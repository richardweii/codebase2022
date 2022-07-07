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

namespace kv {

class MemTable NOCOPYABLE {
  struct InternalKeyHash {
    size_t operator()(const Key &key) const { return Hash(key->c_str(), key->size(), 0x1237423); }
  };

  struct InternalKeyEqual {
    bool operator()(const Key &a, const Key &b) const { return *a == *b; }
  };

 public:
  MemTable(int cap = kItemNum) : cap_(cap) {}

  bool Insert(Key key, Value value) {
    LOG_ASSERT(count_ < cap_, "Insert too many items to memtable, need build a block.");
    if (!table_.count(key)) {
      count_++;
    }
    table_[key] = value;
    return true;
  }

  bool Full() const { return count_ >= cap_; }

  Value Read(Key key) {
    if (table_.count(key)) {
      return table_[key];
    }
    return nullptr;
  }

  DataBlock *BuildDataBlock(DataBlock *datablock);

  void Reset() {
    table_.clear();
    count_ = 0;
  }

 private:
  std::unordered_map<Key, Value, InternalKeyHash, InternalKeyEqual> table_;
  int count_ = 0;
  int cap_;
};

}  // namespace kv