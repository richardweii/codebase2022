#pragma once

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include "config.h"
#include "kv_engine.h"
#include "test.h"
#include "util/logging.h"

using namespace kv;
using namespace std;

#define OUTPUT(format, ...) printf(format, ##__VA_ARGS__)
#define ASSERT(condition, format, ...)                                                                      \
  if (!(condition)) {                                                                                       \
    OUTPUT("\033[;31mAssertion ' %s ' Failed!\n%s:%d: " format "\n\033[0m", #condition, __FILE__, __LINE__, \
           ##__VA_ARGS__);                                                                                  \
    exit(1);                                                                                                \
  }

#define EXPECT(condition, format, ...)                                                                            \
  if (!(condition)) {                                                                                             \
    OUTPUT("\033[;33mExpect ' %s ' \n%s:%d: " format "\n\033[0m", #condition, __FILE__, __LINE__, ##__VA_ARGS__); \
  }

inline vector<Key> genKey(int num) {
  std::mt19937 gen;
  gen.seed(std::random_device()());
  std::uniform_int_distribution<std::mt19937::result_type> dist;
  std::vector<Key> keys;
  for (int i = 0; i < num; i++) {
    keys.emplace_back(new std::string());
    keys.back()->resize(kKeyLength);
    uint8_t *data = (uint8_t *)(keys.back()->c_str());
    for (int i = 0; i < kKeyLength; i++) {
      data[i] = (dist(gen) % 26) + 'a';
    }
  }
  return keys;
}

inline vector<Value> genValue(int num) {
  std::mt19937 gen;
  gen.seed(std::random_device()());
  std::uniform_int_distribution<std::mt19937::result_type> dist;
  std::vector<Key> keys;
  for (int i = 0; i < num; i++) {
    keys.emplace_back(new std::string());
    keys.back()->resize(kValueLength);
    uint8_t *data = (uint8_t *)(keys.back()->c_str());
    for (int i = 0; i < kValueLength; i++) {
      data[i] = (dist(gen) % 26) + 'a';
    }
  }
  return keys;
}