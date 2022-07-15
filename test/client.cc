#include <sys/socket.h>
#include <cstring>
#include <string>
#include <thread>
#include "kv_engine.h"
#include "logging.h"
#include "msg.h"
#include "rdma_client.h"

using namespace kv;

int main() {
  LocalEngine *engine = new LocalEngine();
  engine->start("192.168.200.22", "12345");

  for (int i = 0; i < 12 * 1024 * 16; i++) {
    std::string key(std::to_string(i));
    key.resize(16);
    std::string value = std::to_string(i * 2);
    value.resize(128);
    auto ret = engine->write(key, value);
    LOG_ASSERT(ret, "Failed to write %s", key.c_str());
  }

  for (int i = 0; i < 12 * 1024 * 16; i++) {
    std::string key(std::to_string(i));
    key.resize(16);
    std::string value;
    value.resize(128);
    std::string expected = std::to_string(i * 2);
    expected.resize(128);
    auto ret = engine->read(key, value);
    LOG_ASSERT(ret, "Failed to read %s", key.c_str());
    LOG_ASSERT(value == expected, "got %s", value.c_str());
  }
}