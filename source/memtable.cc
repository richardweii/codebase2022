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

  // 先生成有序对
  std::vector<std::pair<Slice, Slice>> items;
  for (auto &&kv : table_) {
    items.emplace_back(kv.first.toSlice(), kv.second.toSlice());
  }

  auto comp = [&](const std::pair<Slice, Slice> &a, const std::pair<Slice, Slice> &b) -> bool {
    return ::strncmp(a.first.data(), b.first.data(), a.first.size()) < 0;
  };

  std::sort(items.begin(), items.end(), comp);

  // 顺序写入到DataBlock
  BlockBuilder builder(datablock);
  int num = 0;
  for (auto &&kv : items) {
    num++;
    builder.Put(std::move(kv.first), std::move(kv.second));
  }

  builder.Finish(this->bloom_filter_);
  return datablock;
}
}  // namespace kv