#include "block.h"
#include <memory>
#include "config.h"
#include "util/logging.h"

void DataBlock::put(InternalKey key, InternalValue value) {
  LOG_ASSERT(pos_ < cap_, "position %d is out of capacity %d.", pos_, cap_);
  LOG_ASSERT(key->size() == kKeyLength, "Invalid key size %lu.", key->size());
  LOG_ASSERT(value->size() == kValueLength, "Invalid value size %lu.", value->size());

  memcpy(current(), key->c_str(), key->size());
  keys_.push_back(Slice(current(), key->size()));
  advance(key->size());

  memcpy(current(), value->c_str(), value->size());
  advance(value->size());
}

void DataBlock::fillFilterData(Ptr<Filter> filter) {
  LOG_ASSERT(pos_ <= (kKeyLength + kValueLength) * kItemNum, "Invalid position %d", pos_);
  pos_ = (kKeyLength + kValueLength) * kItemNum;
  filter->CreateFilter(&keys_[0], keys_.size(), current());
  LOG_ASSERT(pos_ == cap_, "Unmatched dataBlock size: pos %d != cap %d", pos_, cap_);
}