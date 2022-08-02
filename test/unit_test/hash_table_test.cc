#include "hash_table.h"
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include "config.h"
#include "test.h"
#include "util/logging.h"
#include "util/slice.h"

struct DummyEntry {
  std::string key;
  std::string value;
};

class DummyHandler : public HashHandler<Slice> {
 public:
  DummyHandler(std::vector<DummyEntry> *data) : data_(data){};
  Slice GetKey(uint64_t data_handle) override { return (*data_)[data_handle].key; };

 private:
  std::vector<DummyEntry> *data_;
};

void TestBasic2() {
  std::vector<DummyEntry> entries;
  DummyHandler *handler = new DummyHandler(&entries);
  HashTable<Slice> table(1023, handler);
  for (int i = 0; i < 1023; i++) {
    entries.emplace_back();
    entries.back().key = to_string(i);
    entries.back().value = to_string(i * 2);
  }
  for (int i = 0; i < 1023; i++) {
    table.Insert(entries[i].key, i);
  }
  LOG_ASSERT(table.Count() == 1023, "expect 1023 got %zu", table.Count());

  for (int i = 0; i < 1023; i++) {
    auto node = table.Find(entries[i].key);
    ASSERT(node != nullptr, "expected %d", i);
    EXPECT(node->Handle() == (uint64_t)i, "got %lu", node->Handle())
  }

  for (int i = 0; i < 1023; i++) {
    table.Remove(entries[i].key);
    auto node = table.Find(entries[i].key);
    ASSERT(node == nullptr, "node %d", i);
    LOG_ASSERT(table.Count() == (uint32_t)1022 - i, "expect %d got %zu", 1022 - i, table.Count());

    for (int j = i + 1; j < 1023; j++) {
      auto node = table.Find(entries[j].key);
      ASSERT(node != nullptr, "");
      EXPECT(node->Handle() == (uint64_t)j, "got %lu", node->Handle())
    }
  }
  LOG_ASSERT(table.Count() == 0, "expect 0 got %zu", table.Count());
  delete handler;
}

int main() { TestBasic2(); }