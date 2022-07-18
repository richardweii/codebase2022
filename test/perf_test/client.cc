#include <unistd.h>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <iterator>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include "config.h"
#include "kv_engine.h"
#include "test.h"
#include "util/logging.h"
#include "zipf.h"

using namespace kv;
using namespace std;

constexpr int thread_num = 16;
constexpr int write_op_per_thread = kKeyNum / thread_num;
constexpr int read_write_mix_op = 64 * 1024 * 1024;

int main() {
  LocalEngine *local_engine = new LocalEngine();
  // ip 必须写具体ip，不能直接写localhost和127.0.0.1
  local_engine->start("192.168.2.101", "12344");
  std::vector<std::thread> threads;

  LOG_INFO(" ============= gen key ===============>");
  auto keys = genKey(kKeyNum);

  LOG_INFO(" ============= start write ==============>");
  {
    auto time_now = TIME_NOW;
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [=](const std::vector<std::string> &k) {
            std::string value;
            value.resize(kValueLength);
            for (int j = 0; j < write_op_per_thread; j++) {
              auto succ = local_engine->write(k[i * write_op_per_thread + j], value);
              EXPECT(succ, "[thread %d] failed to write %d", gettid(), i * write_op_per_thread + j);
            }
          },
          keys);
    }
    for (auto &th : threads) {
      th.join();
    }
    auto time_end = TIME_NOW;
    auto time_delta = time_end - time_now;
    auto count = std::chrono::duration_cast<std::chrono::microseconds>(time_delta).count();
    std::cout << "Total time:" << count * 1.0 / 1000 / 1000 << "s" << std::endl;
  }

  LOG_INFO(" ============= start read & write ===============>");
  {
    mt19937 gen;
    gen.seed(random_device()());
    uniform_int_distribution<mt19937::result_type> dist;

    auto time_now = TIME_NOW;
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [&](const std::vector<std::string> &k) {
            std::string write_value;
            write_value.resize(kValueLength);

            std::string read_value;
            for (int i = 0; i < read_write_mix_op; i++) {
              auto prob = dist(gen) % 8;
              auto index = zipf(kKeyNum) - 1;
              if (prob == 0) {
                // wirte;
                auto succ = local_engine->write(keys[index], write_value);
                EXPECT(succ, "MIX [thread %d] failed to write %d", gettid(), index);
              } else {
                // read
                auto succ = local_engine->read(keys[index], read_value);
                EXPECT(succ, "MIX [thread %d] failed to read %d", gettid(), index);
              }
            }
          },
          keys);
    }
    for (auto &th : threads) {
      th.join();
    }
    auto time_end = TIME_NOW;
    auto time_delta = time_end - time_now;
    auto count = std::chrono::duration_cast<std::chrono::microseconds>(time_delta).count();
    std::cout << "Total time:" << count * 1.0 / 1000 / 1000 << "s" << std::endl;
  }
  local_engine->stop();
  delete local_engine;
  return 0;
}