#include "block_builder.h"
#include <memory>
#include "config.h"
#include "util/logging.h"
namespace kv {

void BlockBuilder::Put(Slice key, Slice value) {
  LOG_ASSERT(pos_ < cap_, "position %d is out of capacity %d.", pos_, cap_);
  LOG_ASSERT(key.size() == kKeyLength, "Invalid key size %zu.", key.size());
  LOG_ASSERT(value.size() == kValueLength, "Invalid value size %zu.", value.size());

  memcpy(current(), key.data(), key.size());
  keys_.push_back(Slice(current(), key.size()));
  advance(key.size());

  memcpy(current(), value.data(), value.size());
  advance(value.size());
}

void BlockBuilder::Finish(Ptr<Filter> filter) {
  LOG_ASSERT(pos_ <= (kKeyLength + kValueLength) * kItemNum, "Invalid position %d", pos_);
  datablock_->SetEntryNum(keys_.size());
  datablock_->SetUsed();
}
}  // namespace kv