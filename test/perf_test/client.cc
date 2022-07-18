#include <unistd.h>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <iterator>
#include <mutex>
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
// constexpr int read_write_mix_op = 64 * 100;
constexpr int read_write_mix_op = 64 * 1024 * 1024;
constexpr int M = 1024 * 1024;

int main() {
  LocalEngine *local_engine = new LocalEngine();
  // ip 必须写具体ip，不能直接写localhost和127.0.0.1
  local_engine->start("192.168.200.22", "12344");
  std::vector<std::thread> threads;
  std::mutex zipf_mutex;

  LOG_INFO(" ============= gen key and zipf index ===============>");
  auto keys = genPerfKey(kKeyNum);
  int *zipf_index = new int[read_write_mix_op * 16];
  for (int i = 0; i < read_write_mix_op * 16; i++) {
    zipf_index[i] = zipf(kKeyNum, 2.12) - 1;
  }

  LOG_INFO(" ============= start write ==============>");
  {
    threads.clear();
    auto time_now = TIME_NOW;
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [=](TestKey *k) {
            LOG_INFO("Start write thread %d", i);
            std::string value;
            value.resize(kValueLength);
            for (int j = 0; j < write_op_per_thread; j++) {
              if (j % M == 0) {
                LOG_INFO("[thread %d] finish write %d kv", i, j);
              }
              auto succ = local_engine->write(k[i * write_op_per_thread + j].to_string(), value);
              EXPECT(succ, "[thread %d] failed to write %d", i, i * write_op_per_thread + j);
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
    auto time_now = TIME_NOW;
    threads.clear();
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [=](TestKey *k, int *zipf_index) {
            // mt19937 gen;
            // gen.seed(random_device()());
            // uniform_int_distribution<mt19937::result_type> dist;
            std::string write_value;
            write_value.resize(kValueLength);

            std::string read_value;
            for (int j = 0; j < read_write_mix_op; j++) {
              auto prob = j % 8;
              if (j % M == 0) {
                LOG_INFO("[thread %d] finish read&wirte %d kv", i, j);
              }
              if (prob == 0) {
                // wirte;
                auto succ = local_engine->write(k[zipf_index[i * read_write_mix_op + j]].to_string(), write_value);
                EXPECT(succ, "MIX [thread %d] failed to write %d", gettid(), zipf_index[i * read_write_mix_op + j]);
              } else {
                // read
                auto succ = local_engine->read(k[zipf_index[i * read_write_mix_op + j]].to_string(), read_value);
                EXPECT(succ, "MIX [thread %d] failed to read %d", gettid(), zipf_index[i * read_write_mix_op + j]);
              }
            }
          },
          keys, zipf_index);
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