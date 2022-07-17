#include "hash_table.h"
#include <cstring>
#include <string>
#include <vector>
#include "config.h"
#include "test.h"
#include "util/logging.h"

struct DummyEntry {
  char key[10] = {};
  std::string value;
};

void TestBasic2() {
  std::vector<DummyEntry> entries;
  HashTable table(1023);
  for (int i = 0; i < 1023; i++) {
    entries.emplace_back();
    memcpy(entries.back().key, to_string(i).c_str(), to_string(i).size());
    entries.back().value = to_string(i * 2);
  }
  for (int i = 0; i < 1023; i++) {
    table.Insert(Slice(entries[i].key, 10), &entries[i]);
  }
  LOG_ASSERT(table.Count() == 1023, "expect 1023 got %zu", table.Count());

  for (int i = 0; i < 1023; i++) {
    auto node = table.Find(Slice(entries[i].key, 10));
    ASSERT(node != nullptr, "");
    EXPECT(memcmp(node->GetEntry<DummyEntry>()->key, entries[i].key, 10) == 0, "got %s",
           node->GetEntry<DummyEntry>()->key)
  }

  for (int i = 0; i < 1023; i++) {
    table.Remove(Slice(entries[i].key, 10));
    auto node = table.Find(Slice(entries[i].key, 10));
    ASSERT(node == nullptr, "node %d", i);
    LOG_ASSERT(table.Count() == (uint32_t)1022 - i, "expect %d got %zu", 1022 - i, table.Count());

    for (int j = i + 1; j < 1023; j++) {
      auto node = table.Find(Slice(entries[j].key, 10));
      ASSERT(node != nullptr, "");
      EXPECT(memcmp(node->GetEntry<DummyEntry>()->key, entries[j].key, 10) == 0, "got %s",
             node->GetEntry<DummyEntry>()->key)
    }
  }
  LOG_ASSERT(table.Count() == 0, "expect 0 got %zu", table.Count());
}

int main() { TestBasic2(); }