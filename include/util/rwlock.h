#pragma once

#include <pthread.h>
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

}  // namespace kv