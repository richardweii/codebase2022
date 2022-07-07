#include "block.h"
#include <cstddef>
#include <string>
#include "config.h"
#include "util/logging.h"

namespace kv {

BlockHandle::BlockHandle(DataBlock *datablock) : datablock_(datablock) {
  items_ = reinterpret_cast<Item *>(datablock_->Data());
}

Value BlockHandle::Read(Key key, Ptr<Filter> filter, CacheEntry &entry) const {
  int index;
  if (find(key, filter, &index)) {
    LOG_ASSERT(index < kItemNum, "Out of bounds.");
    entry.id = this->GetBlockId();
    entry.off = index;
    entry.handle = const_cast<BlockHandle*>(this);
    return Read(index);
  }
  return nullptr;
}

Value BlockHandle::Read(size_t off) const {
  LOG_ASSERT(off < EntryNum(), "Out of range.");
  Lock(off);
  Value val(new std::string(items_[off].value, kValueLength));
  Unlock(off);
  return val;
}

bool BlockHandle::Modify(size_t off, Value value) {
  LOG_ASSERT(off < EntryNum(), "Out of range.");
  LOG_ASSERT(value->size() == kValueLength, "Invalid Value %s", value->c_str());
  Lock(off);
  memcpy(items_[off].value, value->c_str(), value->size());
  Unlock(off);
  return true;
}

bool BlockHandle::Modify(Key key, Value value, Ptr<Filter> filter, CacheEntry &entry) {
  int index;
  if (find(key, filter, &index)) {
    entry.id = this->GetBlockId();
    entry.off = index;
    entry.handle = this;
    return Modify(index, value);
  }
  return false;
}

bool BlockHandle::find(Key key, Ptr<Filter> filter, int *index) const {
  // filter
  if (!filter->KeyMayMatch(Slice(*key), Slice(filter_data_, kFilterSize))) {
    return false;
  }
  // search
  int res = binarySearch(key);
  if (index != nullptr) {
    *index = res;
  }
  return res != -1;
}

int BlockHandle::binarySearch(Key key) const {
  int left = 0, right = EntryNum();
  while (left <= right) {
    int mid = left + (right - left) / 2;
    int cmp = strcmp(items_[mid].key, key->c_str());
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

void CacheDeleter(const Slice &key, void *value) {
  CacheEntry *entry = (CacheEntry *)value;
  delete entry;
}


}  // namespace kv