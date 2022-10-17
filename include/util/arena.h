#pragma once
#include <cassert>
#include <cstdlib>
#include <deque>
#include <mutex>
#include <new>
#include <stdexcept>
#include <vector>
#include "defer.h"
#include "logging.h"
#include "nocopy.h"
#include "rwlock.h"

/**
 * @brief a simple memory pool for frequent variable allocate & deallocate
 *
 */

namespace kv {

class Arena NOCOPYABLE {
 private:
  struct Block {
    std::atomic<uint32_t> ref;
    char *base;
  };

  static const uint32_t INVALID_BLOCK = -1U;
  static const size_t INVALID_POS = -1UL;

  struct threadData {
    uint32_t m_bid = INVALID_BLOCK;
    size_t m_pos = INVALID_POS;
  };

  threadData &threadInstance() {
    thread_local threadData tdata;
    return tdata;
  }

  uint32_t ptr2bid(const char *ptr) const { return (ptr - memoryBase()) / BLOCK_SIZE; }

  uint32_t allocBlock() {
    uint32_t bid;
    {
      _lock.Lock();
      if (_free_block_queue.empty()) {
        LOG_FATAL("no more block.");
        _lock.Unlock();
        return INVALID_BLOCK;
      }
      bid = _free_block_queue.back();
      _free_block_queue.pop_back();
      _lock.Unlock();
    }
    Block &m_base = _blocks[bid];
    m_base.ref = ALLOCING;
    return bid;
  }

  char *memoryBase() const { return _blocks[0].base; }

  void init(size_t nbytes) {
    assert(nbytes % BLOCK_SIZE == 0);
    _nbytes = nbytes;
    size_t bcount = (nbytes + BLOCK_SIZE - 1) / BLOCK_SIZE;
    _blocks = new Block[bcount];
    _block_lock = new SpinLock[bcount];

    char *mr = static_cast<char *>(::operator new(nbytes));

    for (size_t i = 0; i < bcount; ++i) {
      Block &b = _blocks[i];
      b.ref = 0;
      b.base = mr + i * BLOCK_SIZE;
    }
    // reduce cache miss at first time
    for (int j = 0; j < 8; ++j) {
      for (size_t i = 0; i < bcount / 8; ++i) {
        _free_block_queue.push_back(i * 8 + j);
      }
    }
    size_t less = bcount - bcount / 8 * 8;
    for (size_t i = 0; i < less; ++i) {
      _free_block_queue.push_back(i);
    }
  }

  Arena() = default;
  ~Arena() {
    ::operator delete(memoryBase());
    delete[] _blocks;
  }

 public:
  // must 4KB aligned
  void Init(size_t nbytes) {
    std::call_once(_flag, [this, nbytes]() { init(nbytes); });
  }

  void *Alloc(size_t bytes) {
    auto &m_bid = threadInstance().m_bid;
    auto &m_pos = threadInstance().m_pos;

    char *ptr;
    if (m_pos + bytes > BLOCK_SIZE || m_bid == INVALID_BLOCK) {
      if (m_bid == INVALID_BLOCK) {
        m_bid = allocBlock();
        m_pos = 0;
      } else {
        Block &m_base = _blocks[m_bid];
        _block_lock[m_bid].Lock();
        if (m_base.ref == ALLOCING) {
          _block_lock[m_bid].Unlock();
          m_pos = 0;
        } else {
          m_base.ref.fetch_xor(ALLOCING);
          _block_lock[m_bid].Unlock();
          m_bid = allocBlock();
          m_pos = 0;
        }
      }
    }
    if (m_bid == INVALID_BLOCK) {
      return nullptr;
    }
    Block &m_base = _blocks[m_bid];
    ptr = m_base.base + m_pos;
    m_pos += bytes;

    m_base.ref.fetch_add(1, std::memory_order_relaxed);
    return ptr;
  }

  template <typename Tp>
  Tp *Alloc() {
    size_t bytes = sizeof(Tp);
    void *ptr = Alloc(bytes);
    return reinterpret_cast<Tp *>(ptr);
  }

  template <typename Tp>
  void Free(Tp *ptr) {
    if (ptr == nullptr) {
      return;
    }
    char *rptr = reinterpret_cast<char *>(ptr);
    if (rptr > memoryBase() + _nbytes || rptr < memoryBase()) LOG_FATAL("out of range");

    uint32_t bid = ptr2bid(rptr);
    Block &m_base = _blocks[bid];
    m_base.ref.fetch_sub(1, std::memory_order_relaxed);
    _block_lock[bid].Lock();
    if (m_base.ref == 0) {
      _lock.Lock();
      _free_block_queue.push_front(bid);
      _lock.Unlock();
    }
    _block_lock[bid].Unlock();
  }

  static Arena &getInstance() {
    static Arena instance;
    return instance;
  }

  static const size_t BLOCK_SIZE = 4096;
  static const uint32_t ALLOCING = 0x70000000;

 private:
  size_t _nbytes;
  SpinLock _lock;
  SpinLock *_block_lock;
  Block *_blocks;
  std::deque<uint32_t> _free_block_queue;
  std::once_flag _flag;
};
}  // namespace kv