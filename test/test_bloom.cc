#include <string>
#include "block.h"
#include "block_builder.h"
#include "config.h"
#include "memtable.h"
#include "test.h"
#include "util/filter.h"

using namespace kv;

int main() {
  auto datablock = new DataBlock();
  MemTable table;

  auto keys = genKey(16);
  auto values = genValue(16);

  for (int i = 0; i < 16; i++) {
    table.Insert(keys[i], values[i]);
  }
  table.BuildDataBlock(datablock);

  BlockHandle handle(datablock);

  CacheEntry entry;
  for (int i = 0; i < 16; i++) {
    auto val = handle.Read(keys[i], NewBloomFilterPolicy(), entry);
    EXPECT(val != nullptr, "Cannot find %s", keys[i]->c_str());
  }
}