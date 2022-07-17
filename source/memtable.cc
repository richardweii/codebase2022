#include "memtable.h"
#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include "block.h"
#include "block_builder.h"
#include "config.h"
#include "util/filter.h"
namespace kv {

DataBlock *MemTable::BuildDataBlock(DataBlock *datablock) {
  LOG_ASSERT(count_ <= cap_, "Memtable has too many items.");
  // 写入到DataBlock
  BlockBuilder builder(datablock);
  int num = 0;
  for (auto &&kv : table_) {
    num++;
    builder.Put(kv.first.toSlice(), kv.second.toSlice());
  }

  builder.Finish(nullptr);
  return datablock;
}
}  // namespace kv