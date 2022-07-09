#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <vector>
#include "block.h"
#include "config.h"
#include "util/filter.h"
#include "util/logging.h"
#include "util/slice.h"

namespace kv {
class BlockBuilder {
 public:
  BlockBuilder(DataBlock *datablock) : datablock_(datablock) {
    LOG_ASSERT(datablock_->IsFree(), "Access of Used datablock.");
    datablock_->SetUsed();
  }

  // 向块中插入
  void Put(Key key, Value value);

  // 生成尾部的过滤器字段
  void Finish(Ptr<Filter> filter);

 private:
  char *current() { return datablock_->Data() + pos_; }

  void advance(int delta) { pos_ += delta; }

  int pos_ = 0;
  int cap_ = kDataSize;
  DataBlock *datablock_;
  std::vector<Slice> keys_;
};
}  // namespace kv