#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include "arena.h"
#include "nocopy.h"
#include "util/likely.h"

namespace kv {

constexpr static uint64_t MASK[] = {
    0,
    0x1ULL,
    0x3ULL,
    0x7ULL,
    0xfULL,
    0x1fULL,
    0x3fULL,
    0x7fULL,
    0xffULL,
    0x1ffULL,
    0x3ffULL,
    0x7ffULL,
    0xfffULL,
    0x1fffULL,
    0x3fffULL,
    0x7fffULL,
    0xffffULL,
    0x1ffffULL,
    0x3ffffULL,
    0x7ffffULL,
    0xfffffULL,
    0x1fffffULL,
    0x3fffffULL,
    0x7fffffULL,
    0xffffffULL,
    0x1ffffffULL,
    0x3ffffffULL,
    0x7ffffffULL,
    0xfffffffULL,
    0x1fffffffULL,
    0x3fffffffULL,
    0x7fffffffULL,
    0xffffffffULL,
    0x1ffffffffULL,
    0x3ffffffffULL,
    0x7ffffffffULL,
    0xfffffffffULL,
    0x1fffffffffULL,
    0x3fffffffffULL,
    0x7fffffffffULL,
    0xffffffffffULL,
    0x1ffffffffffULL,
    0x3ffffffffffULL,
    0x7ffffffffffULL,
    0xfffffffffffULL,
    0x1fffffffffffULL,
    0x3fffffffffffULL,
    0x7fffffffffffULL,
    0xffffffffffffULL,
    0x1ffffffffffffULL,
    0x3ffffffffffffULL,
    0x7ffffffffffffULL,
    0xfffffffffffffULL,
    0x1fffffffffffffULL,
    0x3fffffffffffffULL,
    0x7fffffffffffffULL,
    0xffffffffffffffULL,
    0x1ffffffffffffffULL,
    0x3ffffffffffffffULL,
    0x7ffffffffffffffULL,
    0xfffffffffffffffULL,
    0x1fffffffffffffffULL,
    0x3fffffffffffffffULL,
    0x7fffffffffffffffULL,
};

class Bitmap NOCOPYABLE {
 public:
  Bitmap() = delete;

  int SetFirstFreePos(Bitmap *index_bitmap) {
    int idx = index_bitmap->FirstFreePos();
    if (idx == -1) return -1;
    uint64_t ffp = __builtin_ffsl(_data[idx] + 1) - 1;
    _data[idx] |= (1UL << ffp);
    if (_data[idx] == UINT64_MAX) index_bitmap->Set(idx);
    return (idx << 6) + ffp;
  }

  bool Test(int index) const {
    assert((uint32_t)index < _bits);
    int n = index / 64;
    int off = index % 64;
    return _data[n] & (1ULL << off);
  }

  void Clear(int index, Bitmap *index_bitmap) {
    assert((uint32_t)index < _bits);
    int n = index / 64;
    int off = index % 64;
    _data[n] &= ~(1ULL << off);
    index_bitmap->IndexClear(n);
  }

  bool Set(int index) {
    assert((uint32_t)index < _bits);
    int n = index / 64;
    int off = index % 64;
    if (_data[n] & (1ULL << off)) {
      return false;
    }
    _data[n] |= (1ULL << off);
    return true;
  }

  uint32_t Cap() const { return _bits; }

  uint32_t Size() const { return _n; }

  uint64_t DataAt(int idx) const { return _data[idx]; }

  /*--- for Index Bitmap ---*/
  // return the first free position of bitmap, return -1 if full
  int FirstFreePos() const {
    for (uint32_t i = 0; i < _n; i++) {
      if (_data[i] == UINT64_MAX) continue;
      int ffp = __builtin_ffsl(_data[i] + 1) - 1;
      return (i << 6) + ffp;
    }
    return -1;
  }

  void IndexClear(int index) {
    assert((uint32_t)index < _bits);
    int n = index / 64;
    int off = index % 64;
    _data[n] &= ~(1ULL << off);
  }

 private:
  friend Bitmap *NewBitmap(uint32_t size);
  uint32_t _bits;
  uint32_t _n;
  uint64_t _data[0];
};

__always_inline Bitmap *NewBitmap(uint32_t bits) {
  static_assert(sizeof(uint64_t) == 8, "sizeof(uint64_t) != 8!!!");
  uint32_t n = (bits + 63) / 64;
  int tmp = bits % 64;
  Bitmap *bitmap = reinterpret_cast<Bitmap *>(Arena::getInstance().Alloc(sizeof(Bitmap) + n * sizeof(uint64_t)));
  bitmap->_bits = bits;
  bitmap->_n = n;
  memset((char *)bitmap->_data, 0, n * sizeof(uint64_t));
  if (tmp != 0) {
    bitmap->_data[n - 1] |= (MASK[64 - tmp] << tmp);
  }
  return bitmap;
}

__always_inline void DeleteBitmap(Bitmap *bitmap) { Arena::getInstance().Free(bitmap); }


/** 并发安全的bitmap 的实现 **/
#define ALIGN_UP(a, siz) (((a) + (siz)-1) & (~((siz)-1)))
#define atomic_xadd(P, V) __sync_fetch_and_add((P), (V))
#define cmpxchg(P, O, N) __sync_bool_compare_and_swap((P), (O), (N))
#define atomic_inc(P) __sync_add_and_fetch((P), 1)
#define atomic_dec(P) __sync_add_and_fetch((P), -1)
#define atomic_add(P, V) __sync_add_and_fetch((P), (V))
#define atomic_set_bit(P, V) __sync_or_and_fetch((P), 1 << (V))
#define atomic_clear_bit(P, V) __sync_and_and_fetch((P), ~(1 << (V)))

class ConBitmap {
 public:
  ConBitmap() = delete;

  bool Full() const { return free_cnt == 0; }

  bool Empty() const { return free_cnt == cnt; }

  int get_free() {
    volatile unsigned long tot, i, j;

    if (UNLIKELY(this->free_cnt == 0)) assert(false);
    this->free_cnt -= 1;

    tot = this->siz / 64;
    for (i = 0; i < tot; i++) {
      if (!(data[i]+1)) continue;
      j = __builtin_ffsl(data[i] + 1) - 1;
      data[i] |= (1UL << j);
      return (i << 6) | j;
    }
    assert(free_cnt >= 0);
    assert(false);

    return -1;
  }

  int con_get_free() {
    unsigned long tot, i, ii, j;
    unsigned long old_free_cnt, old_val;
    do {
      old_free_cnt = this->free_cnt;
      if (UNLIKELY(old_free_cnt == 0)) return -1;
    } while (UNLIKELY(!free_cnt.compare_exchange_strong(old_free_cnt, old_free_cnt - 1)));

    tot = this->siz / 64;
    for (i = 0; i < tot; i++) {
      for (;;) {
        old_val = this->data[i];
        if (old_val == (unsigned long)-1) break;
        j = __builtin_ffsl(old_val + 1) - 1;
        if (cmpxchg(&this->data[i], old_val, old_val | (1UL << j))) return (i << 6) | j;
      }
    }
    return -1;
  }

  void put_back(int bk) {
    unsigned long old_val;
    // assert((this->data[bk >> 6] >> (bk & 63)) & 1);
    do {
      old_val = this->data[bk >> 6];
    } while (UNLIKELY(!cmpxchg(&this->data[bk >> 6], old_val, old_val ^ (1UL << (bk & 63)))));
    // atomic_inc(&this->free_cnt);
    free_cnt++;
  }

 private:
  friend Bitmap *NewBitmap(uint32_t size);
  unsigned long cnt, siz;
  std::atomic<unsigned long> free_cnt;
  unsigned long data[0];
};
}  // namespace kv