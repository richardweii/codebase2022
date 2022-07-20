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
#include "util/filter.h"
#include "util/hash.h"
#include "util/logging.h"
#include "util/nocopy.h"

namespace kv {

class MemTable NOCOPYABLE {
 public:
  MemTable(int cap = kItemNum) : cap_(cap) { filter_ = NewBloomFilterPolicy(); }
  ~MemTable() { delete filter_; }
  bool Insert(Slice key, Slice value) {
    Key k(key);
    if (!table_.count(k)) {
      LOG_ASSERT(count_ < cap_, "Insert too many items to memtable, need build a block.");
      count_++;
    }
    table_[std::move(k)] = Value(value);
    return true;
  }

  bool Full() const { return count_ >= cap_; }

  bool Read(Slice key, std::string &value) {
    Key k(key);
    if (table_.count(k)) {
      value.resize(kValueLength);
      memcpy((char *)value.c_str(), table_[std::move(k)].data(), kValueLength);
      return true;
    }
    return false;
  }

  bool Exist(Slice key) { return table_.count(Key(key)); }

  DataBlock *BuildDataBlock(DataBlock *datablock);

  void Reset() {
    table_.clear();
    count_ = 0;
  }

 private:
  std::unordered_map<Key, Value, Key::InternalHash, Key::InternalEqual> table_;
  int count_ = 0;
  int cap_;
  Filter *filter_;
};

}  // namespace kv