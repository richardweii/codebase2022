#ifndef __SPSC_BOUNDED_QUEUE_INCLUDED__
#define __SPSC_BOUNDED_QUEUE_INCLUDED__

#include <assert.h>
#include <atomic>
#include <cstddef>

template <typename T>
class LFQueue {
 public:
  LFQueue(size_t size)
      : _size(size),
        _mask(size - 1),
        _buffer(reinterpret_cast<T *>(new aligned_t[_size + 1])),  // need one extra element for a guard
        _head(0),
        _tail(0) {
    // make sure it's a power of 2
    assert((_size != 0) && ((_size & (~_size + 1)) == _size));
  }

  // ~LFQueue() { delete[] _buffer; }

  bool enqueue(T &input) {
    const size_t head = _head.load(std::memory_order_relaxed);

    if (((_tail.load(std::memory_order_acquire) - (head + 1)) & _mask) >= 1) {
      _buffer[head & _mask] = input;
      _head.store(head + 1, std::memory_order_release);
      return true;
    }
    return false;
  }

  bool dequeue(T &output) {
    const size_t tail = _tail.load(std::memory_order_relaxed);

    if (((_head.load(std::memory_order_acquire) - tail) & _mask) >= 1) {
      output = _buffer[_tail & _mask];
      _tail.store(tail + 1, std::memory_order_release);
      return true;
    }
    return false;
  }

 private:
  typedef typename std::aligned_storage<sizeof(T), std::alignment_of<T>::value>::type aligned_t;
  typedef char cache_line_pad_t[64];

  cache_line_pad_t _pad0;
  const size_t _size;
  const size_t _mask;
  T *const _buffer;

  cache_line_pad_t _pad1;
  std::atomic<size_t> _head;

  cache_line_pad_t _pad2;
  std::atomic<size_t> _tail;

  LFQueue(const LFQueue &) {}
  void operator=(const LFQueue &) {}
};

#endif