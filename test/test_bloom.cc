#include <string>
#include "block.h"
#include "block_builder.h"
#include "config.h"
#include "memtable.h"
#include "slice.h"
#include "test.h"
#include "util/filter.h"

using namespace kv;

int main() {
  auto datablock = new DataBlock();
  MemTable table;

  auto keys = genKey(8);
  auto values = genValue(8);

  for (int i = 0; i < 8; i++) {
    table.Insert(Slice(keys[i]), Slice(values[i]));
  }
  table.BuildDataBlock(datablock);

  BlockHandle handle(datablock);

  CacheEntry entry;
  for (int i = 0; i < 8; i++) {
    std::string value;
    bool succ = handle.Read(Slice(keys[i]), value, NewBloomFilterPolicy(), entry);
    EXPECT(succ, "Cannot find %s", keys[i].c_str());
  }
}