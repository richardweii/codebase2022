#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>
#include "config.h"
#include "util/coding.h"
#include "util/filter.h"
#include "util/logging.h"
#include "util/nocopy.h"
#include "util/slice.h"

namespace kv {

static constexpr size_t kBlockMetaDataSize = 9;

enum DataBlockState {
  FREE,
  USED,
};

/**
 * |--------------------------------------------------------------------------------------------------
 * |  key |    value | key |     value   | ......... |    filter    |     padding     | Meta Data    |
 * |--------------------------------------------------------------------------------------------------
 * |  - - - - - -     kDataSize  - - - - - - - - - - | kFilterSize  |   empty bytes  |  last 6 bytes |
 *
 *              |-------------------|--------------|----------|
 * meta data    |   entry num       |       id     |   state  |
 *              |-------------------|--------------|----------|
 *
 */
class DataBlock NOCOPYABLE {
 public:
  DataBlock() {
    static_assert(kDataBlockSize - kDataSize - kFilterSize >= kBlockMetaDataSize, "No space for Meta Data.");
  }

  char *Data() { return data_; }

  void Free() {
    memset(data_, 0, kDataBlockSize);
    SetId(INVALID_BLOCK_ID);
    SetEntryNum(0);
    data_[kDataBlockSize - 1] = FREE;
  }

  bool IsFree() const { return data_[kDataBlockSize - 1] == FREE; }

  void SetUsed() { data_[kDataBlockSize - 1] = USED; }

  BlockId GetId() const { return DecodeFixed32(data_ + kDataBlockSize - kBlockMetaDataSize - 4); }

  void SetId(BlockId id) { EncodeFixed32(data_ + kDataBlockSize - kBlockMetaDataSize - 4, id); }

  uint32_t GetEntryNum() const { return DecodeFixed32(data_ + kDataBlockSize - kBlockMetaDataSize); }

  void SetEntryNum(uint32_t num) { EncodeFixed32(data_ + kDataBlockSize - kBlockMetaDataSize, num); }

 private:
  char data_[kDataBlockSize];
};

struct CacheEntry;

class BlockHandle {
 public:
  struct Item {
    char key[kKeyLength];
    char value[kValueLength];
  };
  BlockHandle(DataBlock *datablock);

  Value Read(Key key, Ptr<Filter> filter, CacheEntry &entry) const;
  bool Modify(Key key, Value value, Ptr<Filter> filter, CacheEntry &entry);

  Value Read(size_t off) const;
  bool Modify(size_t off, Value value);

  void Lock(size_t item_index) const {
    while (locks_[item_index].test_and_set())
      ;
  }
  void Unlock(size_t item_index) const { locks_[item_index].clear(); }

  BlockId GetBlockId() const { return datablock_->GetId(); }

  bool Valid() const { return !datablock_->IsFree(); }

  uint32_t EntryNum() const { return datablock_->GetEntryNum(); }

 private:
  bool find(Key key, Ptr<Filter> filter, int *index = nullptr) const;
  int binarySearch(Key key) const;

  mutable std::atomic_flag locks_[kItemNum] = {ATOMIC_FLAG_INIT};
  DataBlock *datablock_ = nullptr;
  Item *items_ = nullptr;
  char *filter_data_ = nullptr;
};

struct CacheEntry {
  size_t off;
  BlockId id;
  BlockHandle *handle;
};

void CacheDeleter(const Slice &key, void *value);


}  // namespace kv