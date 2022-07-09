#include <cstdint>
#include <cstdio>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include "config.h"
#include "kv_engine.h"
#include "test.h"
#include "util/logging.h"

using namespace kv;
using namespace std;

constexpr int key_num = kKeyNum;
constexpr int write_thread = 1;
constexpr int read_thread = 1;

int main() {
  LocalEngine *local_engine = new LocalEngine();
  local_engine->start("192.168.2.102",
                      "12344");  // ip 必须写具体ip，不能直接写localhost和127.0.0.1
  auto keys = genKey(key_num);
  auto values = genValue(key_num);

  LOG_INFO(" ============= start write ================");
  auto op_per_thread = key_num / write_thread;

  std::vector<std::thread> threads;
  for (int i = 0; i < write_thread; i++) {
    // threads.emplace_back([=]() {
    for (int j = 0; j < op_per_thread; j++) {
      local_engine->write(keys[j + i * op_per_thread], values[j + i * op_per_thread]);
    }
    // });
  }

  for (auto &th : threads) {
    th.join();
  }

  threads.clear();

  LOG_INFO(" ============= start read ================");
  op_per_thread = key_num / read_thread;

  for (int i = 0; i < read_thread; i++) {
    for (int j = 0; j < op_per_thread; j++) {
      // threads.emplace_back([=]() {
      std::string value;
      bool found = local_engine->read(keys[j + i * op_per_thread], value);
      EXPECT(found, "Read %s failed.", keys[j + i * op_per_thread].c_str());
      ASSERT(found && value == values[j + i * op_per_thread], "Unexpected value %s ", value.c_str());
      // });
    }
  }

  for (auto &th : threads) {
    th.join();
  }
  return 0;
}