#include <cstdint>
#include <cstdio>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include "config.h"
#include "kv_engine.h"
#include "util/logging.h"
#include "test.h"

using namespace kv;
using namespace std;

constexpr int key_num = 12 * 16 * 32;

int main() {
  LocalEngine *local_engine = new LocalEngine();
  local_engine->start("172.16.5.129",
                      "12344");  // ip 必须写具体ip，不能直接写localhost和127.0.0.1
  auto keys = genKey(key_num);
  auto values = genValue(key_num);

  LOG_INFO(" ============= start write ================");
  for (int i = 0; i < keys.size(); i++) {
    local_engine->write(*keys[i], *values[i]);
  }
  LOG_INFO(" ============= start read ================");
  for (int i = 0; i < keys.size(); i++) {
    std::string value;
    bool found = local_engine->read(*keys[i], value);
    if (found) {
      LOG_INFO("FOUND !");
    }
    EXPECT(found, "Read %s failed.",keys[i]->c_str());
  }

  return 0;
}