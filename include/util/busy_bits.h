#pragma once
#include <strings.h>
#include <atomic>
#include <random>

class BusyBits {
 public:
  BusyBits(int count) : count_(count) {}

  // 获得目前空闲的bit，同时会将其置为busy状态
  int GetIdleBit() {
    uint64_t busy_bits_ = busy_bits.load(std::memory_order_relaxed);
    uint8_t _tid_ = ffsll(~busy_bits_);
    if (_tid_ == 0 || _tid_ > count_) {
      _tid_ = rng() % count_;  // no unused bit
    } else {
      --_tid_;  // _tid_ is unused
    }
    busy_bits.fetch_and(~(1UL << _tid_), std::memory_order_relaxed);
    return _tid_;
  }

  // 将处于busy状态的bit位变为空闲状态
  void UnsetBit(int tid) { busy_bits.fetch_or(1UL << tid, std::memory_order_relaxed); }

 private:
  std::mt19937 rng;
  std::atomic<uint64_t> busy_bits{0};
  int count_;
};