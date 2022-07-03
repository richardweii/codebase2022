#include "memtable.h"
#include <algorithm>
#include <memory>
#include <string>
#include "block.h"
#include "util/filter.h"

Ptr<DataBlock> MemTable::buildDataBlock() {
  LOG_ASSERT(count_ <= cap_, "Memtable has too many items.");

  // 先生成有序对
  std::vector<std::pair<InternalKey, InternalValue>> items;
  for (auto &kv : table_) {
    items.emplace_back(kv);
  }

  auto comp = [&](std::pair<InternalKey, InternalValue> &a, std::pair<InternalKey, InternalValue> &b) -> bool {
    return *(a.first) < *(b.first);
  };

  std::sort(items.begin(), items.end(), comp);

  // 顺序写入到DataBlock
  DataBlock *block = new DataBlock();
  for (auto &kv : items) {
    block->put(kv.first, kv.second);
  }

  block->fillFilterData(NewBloomFilterPolicy());
  return Ptr<DataBlock>(block);
}