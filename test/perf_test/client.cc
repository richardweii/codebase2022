#include <cstdint>
#include <cstdio>
#include <cstring>
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

constexpr int insert_num = 160000000;
constexpr int thread_num = 16;
constexpr int write_op_per_thread = insert_num / thread_num;
// constexpr int read_write_mix_op = 64 * 100;
constexpr int read_write_mix_op = 64 * 1024 * 1024;
constexpr int M = 1024 * 1024;

// encryption & decrption

// part 1 validate
// write 16 * 10M
// read 16 * 10M
// write 16 * 10M
// read 16 * 10M
// update 16 * 1M
// read ? < 1M

// part 2 delete
// delete 16 * 9M
// read deleted 16 * 9M
// write 16 * 9M

// part 3 hot data
// read 16 * 32M



void part1(LocalEngine *local_engine, TestKey *keys, int *zipf_index, int *key_slab_class,
           std::vector<std::thread> &threads) {
  LOG_INFO(" ============= part1 ==============>");
  {
    LOG_INFO(" @@@@@@@@@@@@@ round 1 @@@@@@@@@@@@@@@");
    auto time_now = TIME_NOW;
    // first write
    threads.clear();
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [=](TestKey *k, int *slab_class) {
            LOG_INFO("Start write thread %d", i);
            std::string value;
            for (int j = 0; j < write_op_per_thread; j++) {
              if (j % M == 0) {
                LOG_INFO("[thread %d] finish write %d kv", i, j);
              }
              int key_idx = j + i * write_op_per_thread;
              uint8_t sc = genValueSlabSize0();
              slab_class[key_idx] = sc;
              value.resize(sc * kSlabSize);
              memcpy((char *)value.c_str(), k[key_idx].key, 16);
              auto succ = local_engine->write(k[key_idx].to_string(), value);
              ASSERT(succ, "[thread %d] failed to write %d", i, key_idx);
            }
            LOG_INFO("End write thread %d", i);
          },
          keys, key_slab_class);
    }
    for (auto &th : threads) {
      th.join();
    }

    // first read
    threads.clear();
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [=](TestKey *k, int *slab_class) {
            LOG_INFO("Start read thread %d", i);
            std::string value;
            for (int j = 0; j < write_op_per_thread; j++) {
              if (j % M == 0) {
                LOG_INFO("[thread %d] finish read %d kv", i, j);
              }
              int key_idx = j + i * write_op_per_thread;
              auto succ = local_engine->read(k[key_idx].to_string(), value);
              ASSERT(succ, "[thread %d] failed to read %d", i, key_idx);
              ASSERT(value.size() == (size_t)slab_class[key_idx] * kSlabSize, "got val length %lu, expected %d",
                     value.size(), slab_class[key_idx] * kSlabSize)
              auto cmp = memcmp(value.c_str(), k[key_idx].key, 16);
              ASSERT(cmp == 0, "expect %.16s, got %.16s", k[key_idx].key, value.c_str());
            }
            LOG_INFO("End read thread %d", i);
          },
          keys, key_slab_class);
    }
    for (auto &th : threads) {
      th.join();
    }

    LOG_INFO(" @@@@@@@@@@@@@ round 2 @@@@@@@@@@@@@@@");
    // second write
    threads.clear();
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [=](TestKey *k, int *slab_class) {
            LOG_INFO("Start write thread %d", i);
            string value;
            for (int j = 0; j < write_op_per_thread; j++) {
              if (j % M == 0) {
                LOG_INFO("[thread %d] finish write %d kv", i, j);
              }
              int key_idx = j + i * write_op_per_thread;
              uint8_t sc = genValueSlabSize1();
              slab_class[key_idx] = sc;
              value.resize(sc * kSlabSize);
              memcpy((char *)value.c_str(), k[key_idx].key, 16);
              auto succ = local_engine->write(k[key_idx].to_string(), value);
              ASSERT(succ, "[thread %d] failed to write %d", i, key_idx);
            }
            LOG_INFO("End write thread %d", i);
          },
          keys, key_slab_class);
    }
    for (auto &th : threads) {
      th.join();
    }

    // second read
    threads.clear();
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [=](TestKey *k, int *slab_class) {
            LOG_INFO("Start read thread %d", i);
            std::string value;
            for (int j = 0; j < write_op_per_thread; j++) {
              if (j % M == 0) {
                LOG_INFO("[thread %d] finish read %d kv", i, j);
              }
              int key_idx = j + i * write_op_per_thread;
              auto succ = local_engine->read(k[key_idx].to_string(), value);
              ASSERT(succ, "[thread %d] failed to read %d", i, key_idx);
              ASSERT(value.size() == (size_t)slab_class[key_idx] * kSlabSize, "got val length %lu, expected %d",
                     value.size(), slab_class[key_idx] * kSlabSize)
              auto cmp = memcmp(value.c_str(), k[key_idx].key, 16);
              ASSERT(cmp == 0, "expect %.16s, got %.16s", k[key_idx].key, value.c_str());
            }
            LOG_INFO("End read thread %d", i);
          },
          keys, key_slab_class);
    }
    for (auto &th : threads) {
      th.join();
    }

    auto time_end = TIME_NOW;
    auto time_delta = time_end - time_now;
    auto count = std::chrono::duration_cast<std::chrono::microseconds>(time_delta).count();
    std::cout << "Total time:" << count * 1.0 / 1000 / 1000 << "s" << std::endl;
  }
}

void part2(LocalEngine *local_engine, TestKey *keys, int *zipf_index, int *key_slab_class,
           std::vector<std::thread> &threads) {
  LOG_INFO(" ============= start read & write ===============>");
  {
    auto time_now = TIME_NOW;
    threads.clear();
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [=](TestKey *k, int *zipf_index, int *slab_class) {
            // mt19937 gen;
            // gen.seed(random_device()());
            // uniform_int_distribution<mt19937::result_type> dist;
            std::string write_value;
            std::string read_value;
            for (int j = 0; j < read_write_mix_op; j++) {
              int key_idx = j + i * read_write_mix_op;
              auto prob = j % 4;
              if (j % (4 * M) == 0) {
                LOG_INFO("[thread %d] finish read&wirte %d M kv", i, j / M);
              }
              if (prob == 0) {
                // wirte;
                write_value.resize(slab_class[zipf_index[key_idx]] * kSlabSize);
                memcpy((char *)write_value.c_str(), k[zipf_index[key_idx]].key, 16);
                auto succ = local_engine->write(k[zipf_index[key_idx]].to_string(), write_value);
                EXPECT(succ, "MIX [thread %d] failed to write %d", i, zipf_index[key_idx]);
              } else {
                // read
                auto succ = local_engine->read(k[zipf_index[key_idx]].to_string(), read_value);
                EXPECT(succ, "MIX [thread %d] failed to read %d", i, zipf_index[key_idx]);
                // auto cmp = memcmp(read_value.c_str(), k[zipf_index[i * read_write_mix_op + j]].key, 16);
                // EXPECT(cmp == 0, "expect %s, got %s", k[zipf_index[i * read_write_mix_op + j]].key,
                //        write_value.c_str());
              }
            }
          },
          keys, zipf_index, key_slab_class);
    }
    for (auto &th : threads) {
      th.join();
    }
    auto time_end = TIME_NOW;
    auto time_delta = time_end - time_now;
    auto count = std::chrono::duration_cast<std::chrono::microseconds>(time_delta).count();
    std::cout << "Total time:" << count * 1.0 / 1000 / 1000 << "s" << std::endl;
  }
}

int main() {
  LocalEngine *local_engine = new LocalEngine();
  // ip 必须写具体ip，不能直接写localhost和127.0.0.1
  local_engine->start("192.168.200.22", "12344");
  std::vector<std::thread> threads;
  std::mutex zipf_mutex;

  LOG_INFO(" ============= gen key and zipf index ===============>");
  auto keys = genKey(insert_num);
  int *key_slab_class = new int[insert_num];
  int *zipf_index = new int[read_write_mix_op * thread_num];
  LOG_INFO(" start gen zipf key...");
  for (int i = 0; i < thread_num; i++) {
    threads.emplace_back(
        [i](int *zipf_index) {
          LOG_INFO("Start gen zipf key index %d", i);
          Zipf zipf(insert_num, 0x123ab324 * (i + 1), 2);
          for (int j = 0; j < read_write_mix_op; j++) {
            zipf_index[read_write_mix_op * i + j] = zipf.Next();
          }
          LOG_INFO("End gen zipf key index %d", i);
        },
        zipf_index);
  }
  for (auto &th : threads) {
    th.join();
  }
  threads.clear();

  LOG_INFO(" end gen zipf key!");

  part1(local_engine, keys, zipf_index, key_slab_class, threads);
  part2(local_engine, keys, zipf_index, key_slab_class, threads);

  delete[] key_slab_class;
  delete[] zipf_index;

  local_engine->stop();
  delete local_engine;
  return 0;
}
