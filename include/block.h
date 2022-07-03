#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <vector>
#include "config.h"
#include "util/filter.h"
#include "util/logging.h"
#include "util/nocopy.h"
#include "util/slice.h"

class DataBlock NOCOPYABLE {
 public:
  DataBlock() { data_ = new char[roundUp(kDataBlockSize)]; }
  ~DataBlock() { delete[] data_; }
  void put(InternalKey key, InternalValue value);

  void fillFilterData(Ptr<Filter> filter);

 private:
  char *current() { return data_ + pos_; }

  void advance(int delta) { pos_ += delta; }

  int pos_ = 0;
  int cap_ = kDataBlockSize;
  char *data_ = nullptr;
  std::vector<Slice> keys_;
};