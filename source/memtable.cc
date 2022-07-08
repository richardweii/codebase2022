#include "memtable.h"
#include <algorithm>
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
  std::vector<std::pair<Key, Value>> items;
  for (auto &kv : table_) {
    items.emplace_back(kv);
  }

  auto comp = [&](std::pair<Key, Value> &a, std::pair<Key, Value> &b) -> bool {
    return *(a.first) < *(b.first);
  };

  std::sort(items.begin(), items.end(), comp);

  // 顺序写入到DataBlock
  BlockBuilder builder(datablock);
  int num = 0;
  for (auto &kv : items) {
    num++;
    builder.Put(kv.first, kv.second);
  }

  builder.Finish(NewBloomFilterPolicy());
  return datablock;
}
}  // namespace kv