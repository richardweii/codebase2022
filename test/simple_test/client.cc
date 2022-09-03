#include <unistd.h>
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
constexpr int thread_num = 4;
constexpr int write_op_num = key_num / thread_num;
constexpr int delete_op_num = write_op_num * 0.8;
constexpr int rewrite_op_num = delete_op_num;
constexpr int modify_length_op_num = rewrite_op_num / 4;

int main() {
  LocalEngine *local_engine = new LocalEngine();
  local_engine->start("192.168.200.22",
                      "12344");  // ip 必须写具体ip，不能直接写localhost和127.0.0.1
  local_engine->set_aes();
  auto keys = genKey(key_num);
  int *key_slab_class = new int[key_num];

  LOG_INFO(" ============= start encrypt ================");
  std::vector<std::thread> threads;
  for (int i = 0; i < thread_num; i++) {
    threads.emplace_back(
        [=](TestKey *keys, int *slab_class) {
          std::string value;
          for (int j = 0; j < write_op_num; j++) {
            int key_idx = j + i * write_op_num;
            uint8_t sc = genValueSlabSize0();
            slab_class[key_idx] = sc;
            value.resize(sc * kSlabSize);
            memcpy((char *)value.data(), keys[key_idx].key, kKeyLength);
            local_engine->write(keys[key_idx].to_string(), value, true);
          }
        },
        keys, key_slab_class);
  }

  for (auto &th : threads) {
    th.join();
  }
  threads.clear();

  LOG_INFO(" ============= start decrypt ================");
  for (int i = 0; i < thread_num; i++) {
    threads.emplace_back(
        [=](TestKey *keys, int *slab_class) {
          for (int j = 0; j < write_op_num; j++) {
            std::string value;
            int key_idx = j + i * write_op_num;
            bool found = local_engine->read(keys[key_idx].to_string(), value);
            ASSERT(found, "Read %.16s failed.", keys[key_idx].key);
            ASSERT(value.size() == (size_t)slab_class[key_idx] * kSlabSize, "got val length %lu, expected %d",
                   value.size(), slab_class[key_idx] * kSlabSize)
            char *val_str = local_engine->decrypt(value.c_str(), value.size());
            ASSERT(memcmp(val_str, keys[key_idx].key, kKeyLength) == 0, "Expected value %s, but %s ", keys[key_idx].key,
                   val_str);
            free(val_str);
          }
        },
        keys, key_slab_class);
  }

  for (auto &th : threads) {
    th.join();
  }
  threads.clear();

  LOG_INFO(" ============= start modify ================");
  for (int i = 0; i < thread_num; i++) {
    threads.emplace_back(
        [=](TestKey *keys, int *slab_class) {
          std::string value;
          for (int j = 0; j < write_op_num; j++) {
            int key_idx = j + i * write_op_num;
            uint8_t sc = slab_class[key_idx];
            value.resize(sc * kSlabSize);
            memcpy((char *)value.data(), keys[key_idx].key, kKeyLength);
            local_engine->write(keys[key_idx].to_string(), value, false);
          }
        },
        keys, key_slab_class);
  }
  for (auto &th : threads) {
    th.join();
  }
  threads.clear();

  LOG_INFO(" ============= start read ================");
  for (int i = 0; i < thread_num; i++) {
    threads.emplace_back(
        [=](TestKey *keys, int *slab_class) {
          for (int j = 0; j < write_op_num; j++) {
            std::string value;
            int key_idx = j + i * write_op_num;
            bool found = local_engine->read(keys[key_idx].to_string(), value);
            ASSERT(found, "Read %.16s failed.", keys[key_idx].key);
            ASSERT(value.size() == (size_t)slab_class[key_idx] * kSlabSize, "got val length %lu, expected %d",
                   value.size(), slab_class[key_idx] * kSlabSize)
            ASSERT(memcmp(value.data(), keys[key_idx].key, kKeyLength) == 0, "idx %d Expected value %s, but %s ",
                   key_idx, keys[key_idx].key, value.data());
          }
        },
        keys, key_slab_class);
  }
  for (auto &th : threads) {
    th.join();
  }
  threads.clear();

  LOG_INFO(" ============= start delete ================ ");
  for (int i = 0; i < thread_num; i++) {
    threads.emplace_back(
        [=](TestKey *keys, int *slab_class) {
          std::string value;
          for (int j = 0; j < delete_op_num; j++) {
            int key_idx = j + i * delete_op_num;
            auto succ = local_engine->deleteK(keys[key_idx].to_string());
            ASSERT(succ, "delete key %.16s failed.", keys[key_idx].key);
          }
        },
        keys, key_slab_class);
  }
  for (auto &th : threads) {
    th.join();
  }
  threads.clear();

  LOG_INFO(" ============= start delete validate ================ ");
  for (int i = 0; i < thread_num; i++) {
    threads.emplace_back(
        [=](TestKey *keys, int *slab_class) {
          std::string value;
          for (int j = 0; j < delete_op_num; j++) {
            int key_idx = j + i * delete_op_num;
            bool found = local_engine->read(keys[key_idx].to_string(), value);
            ASSERT(!found, "delete key %.16s failed.", keys[key_idx].key);
          }
        },
        keys, key_slab_class);
  }
  for (auto &th : threads) {
    th.join();
  }
  threads.clear();

  LOG_INFO(" ============= start rewrite ================");
  for (int i = 0; i < thread_num; i++) {
    threads.emplace_back(
        [=](TestKey *keys, int *slab_class) {
          std::string value;
          for (int j = 0; j < rewrite_op_num; j++) {
            int key_idx = j + i * rewrite_op_num;
            uint8_t sc = genValueSlabSize1();
            slab_class[key_idx] = sc;
            value.resize(sc * kSlabSize);
            memcpy((char *)value.data(), keys[key_idx].key, kKeyLength);
            local_engine->write(keys[key_idx].to_string(), value, false);
          }
        },
        keys, key_slab_class);
  }

  for (auto &th : threads) {
    th.join();
  }
  threads.clear();

  LOG_INFO(" ============= start read ================");
  for (int i = 0; i < thread_num; i++) {
    threads.emplace_back(
        [=](TestKey *keys, int *slab_class) {
          for (int j = 0; j < rewrite_op_num; j++) {
            std::string value;
            int key_idx = j + i * rewrite_op_num;
            bool found = local_engine->read(keys[key_idx].to_string(), value);
            ASSERT(found, "Read %.16s failed.", keys[key_idx].key);
            ASSERT(value.size() == (size_t)slab_class[key_idx] * kSlabSize, "got val length %lu, expected %d",
                   value.size(), slab_class[key_idx] * kSlabSize)
            ASSERT(memcmp(value.data(), keys[key_idx].key, kKeyLength) == 0, "Expected value %s, but %s ",
                   keys[key_idx].key, value.data());
          }
        },
        keys, key_slab_class);
  }
  for (auto &th : threads) {
    th.join();
  }
  threads.clear();

  LOG_INFO(" ============= start modify length ================");
  for (int i = 0; i < thread_num; i++) {
    threads.emplace_back(
        [=](TestKey *keys, int *slab_class) {
          std::string value;
          for (int j = 0; j < modify_length_op_num; j++) {
            int key_idx = j + i * modify_length_op_num;
            uint8_t sc = genValueSlabSize1();
            slab_class[key_idx] = sc;
            value.resize(sc * kSlabSize);
            memcpy((char *)value.data(), keys[key_idx].key, kKeyLength);
            local_engine->write(keys[key_idx].to_string(), value, false);
          }
        },
        keys, key_slab_class);
  }

  for (auto &th : threads) {
    th.join();
  }
  threads.clear();

  LOG_INFO(" ============= start read ================");
  for (int i = 0; i < thread_num; i++) {
    threads.emplace_back(
        [=](TestKey *keys, int *slab_class) {
          for (int j = 0; j < modify_length_op_num; j++) {
            std::string value;
            int key_idx = j + i * modify_length_op_num;
            bool found = local_engine->read(keys[key_idx].to_string(), value);
            ASSERT(found, "Read %.16s failed.", keys[key_idx].key);
            ASSERT(value.size() == (size_t)slab_class[key_idx] * kSlabSize, "got val length %lu, expected %d",
                   value.size(), slab_class[key_idx] * kSlabSize)
            ASSERT(memcmp(value.data(), keys[key_idx].key, kKeyLength) == 0, "idx %d, Expected value %s, but %s ",
                   key_idx, keys[key_idx].key, value.data());
          }
        },
        keys, key_slab_class);
  }
  for (auto &th : threads) {
    th.join();
  }
  threads.clear();

  local_engine->stop();
  delete local_engine;
  return 0;
}