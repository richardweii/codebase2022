#pragma once

#include <pthread.h>
#include <atomic>
#include <cstdint>
namespace kv {

/**
 * Reader-Writer latch backed by pthread.h
 */
class Latch {
 public:
  Latch() { pthread_rwlock_init(&rwlock_, nullptr); };
  ~Latch() { pthread_rwlock_destroy(&rwlock_); }

  /**
   * Acquire a write latch.
   */
  int WLock() { return pthread_rwlock_wrlock(&rwlock_); }

  /**
   * Release a write latch.
   */
  int WUnlock() { return pthread_rwlock_unlock(&rwlock_); }

  /**
   * Acquire a read latch.
   */
  int RLock() { return pthread_rwlock_rdlock(&rwlock_); }

  /**
   * Try to acquire a read latch.
   */
  bool tryRLock() { return !pthread_rwlock_tryrdlock(&rwlock_); }

  /**
   * Release a read latch.
   */
  int RUnlock() { return pthread_rwlock_unlock(&rwlock_); }

 private:
  pthread_rwlock_t rwlock_;
};

class SpinLatch {
 public:
  void WLock() {
    int8_t lock = 0;
    while (!lock_.compare_exchange_weak(lock, -1, std::memory_order_acquire)) {
      lock = 0;
    }
  }

  void WUnlock() { lock_.store(0, std::memory_order_release); }

  void RLock() {
    while (true) {
      int8_t lock = lock_.load();
      if (lock + 1 > 0) {
        if (lock_.compare_exchange_weak(lock, lock + 1, std::memory_order_acquire)) {
          return;
        }
      }
      // retry
    }
  }

  void RUnlock() { lock_.fetch_add(-1, std::memory_order_release); }

 private:
  std::atomic_int8_t lock_{0};
};

}  // namespace kv