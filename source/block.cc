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
  filter_data_ = datablock_->Data() + kDataSize;
}

bool BlockHandle::Read(Slice key, std::string &value, Filter * filter, CacheEntry &entry) const {
  assert(key.size() == kKeyLength);
  int index;
  if (Find(key, filter, &index)) {
    LOG_ASSERT(index < kItemNum, "Out of bounds.");
    entry.id = this->GetBlockId();
    entry.off = index;
    return Read(index, value);
  }
  return false;
}

Entry * BlockHandle::ReadEntry(size_t off) const {
  // LOG_ASSERT(off < EntryNum(), "Out of range.");
  return &items_[off];
}

bool BlockHandle::Read(size_t off, std::string &value) const {
  // LOG_ASSERT(off < EntryNum(), "Out of range.");
  Lock(off);
  value.resize(kValueLength);
  memcpy((char *)value.c_str(), items_[off].value, kValueLength);
  Unlock(off);
  return true;
}

bool BlockHandle::Modify(Slice key, Slice value, Filter * filter, CacheEntry &entry) {
  assert(key.size() == kKeyLength);
  assert(value.size() == kValueLength);
  int index;
  if (Find(key, filter, &index)) {
    entry.id = this->GetBlockId();
    entry.off = index;
    return Modify(index, value);
  }
  return false;
}

bool BlockHandle::Modify(size_t off, Slice value) {
  // LOG_ASSERT(off < EntryNum(), "Out of range.");
  LOG_ASSERT(value.size() == kValueLength, "Invalid Value %s", value.data());
  Lock(off);
  memcpy(items_[off].value, value.data(), value.size());
  Unlock(off);
  return true;
}

bool BlockHandle::Find(Slice key, Filter * filter, int *index) const {
  // filter
  if (!filter->KeyMayMatch(key, Slice(filter_data_, (this->EntryNum() * kBloomFilterBitsPerKey + 7) / 8))) {
    return false;
  }
  // search
  int res = binarySearch(key);
  if (index != nullptr) {
    *index = res;
  }
  return res != -1;
}

int BlockHandle::binarySearch(Slice key) const {
  int left = 0, right = EntryNum();
  while (left <= right) {
    int mid = left + (right - left) / 2;
    int cmp = strncmp(items_[mid].key, key.data(), key.size());
    if (cmp == 0) {
      return mid;
    } else if (cmp < 0) {
      left = mid + 1;
    } else if (cmp > 0) {
      right = mid - 1;
    }
  }
  return -1;
}

}  // namespace kv