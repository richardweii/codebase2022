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

  auto keys = genKey(8);
  auto values = genValue(8);

  for (int i = 0; i < 8; i++) {
    table.Insert(Key(new std::string(keys[i])), Key(new std::string(values[i])));
  }
  table.BuildDataBlock(datablock);

  BlockHandle handle(datablock);

  CacheEntry entry;
  for (int i = 0; i < 8; i++) {
    auto val = handle.Read(Key(new std::string(keys[i])), NewBloomFilterPolicy(), entry);
    EXPECT(val != nullptr, "Cannot find %s", keys[i].c_str());
  }
}