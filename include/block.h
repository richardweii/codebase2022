#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include "config.h"
#include "internal.h"
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
  DataBlock() { static_assert(kDataBlockSize - kDataSize >= kBlockMetaDataSize, "No space for Meta Data."); }

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

struct Entry {
  char key[kKeyLength];
  char value[kValueLength];
};

class BlockHandle {
 public:
  BlockHandle(DataBlock *datablock);
  Entry *Read(size_t off) const;

  BlockId GetBlockId() const { return datablock_->GetId(); }

  bool Valid() const { return !datablock_->IsFree(); }

  uint32_t EntryNum() const { return datablock_->GetEntryNum(); }

 private:
  DataBlock *datablock_ = nullptr;
  Entry *items_ = nullptr;
};
}  // namespace kv