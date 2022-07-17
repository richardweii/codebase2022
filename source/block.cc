#include "block.h"
#include <cassert>
#include <cstddef>
#include <string>
#include "config.h"
#include "cache.h"
#include "util/logging.h"
#include "util/slice.h"

namespace kv {

BlockHandle::BlockHandle(DataBlock *datablock) : datablock_(datablock) {
  items_ = reinterpret_cast<Entry *>(datablock_->Data());
}

Entry * BlockHandle::Read(size_t off) const {
  LOG_ASSERT(off < EntryNum(), "Out of range.");
  return &items_[off];
}


}  // namespace kv