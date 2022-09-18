#pragma once

#include <pthread.h>
#include <atomic>
#include <mutex>
#include <set>
#include "util/logging.h"

namespace kv {

class BindCore {
 public:
  BindCore() : done(false), cpu_id(0) {}
  void bind() {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    std::lock_guard<std::mutex> lck(mu);
    if (cpu_id == 16) {
      done = true;
      return;
    }
    CPU_SET(cpu_id, &cpuset);

    auto thread_id = pthread_self();
    if (threadid_set.count(thread_id) == 0) {
      int rc = pthread_setaffinity_np(thread_id, sizeof(cpu_set_t), &cpuset);
      // LOG_INFO("bind thread %ld to core %d", thread_id, cpu_id.load());
      cpu_id++;
      if (cpu_id == 16) {
        done = true;
      }
      threadid_set.insert(thread_id);
    }
  }

  bool isDone() {
    return done;
  }

  bool rebind() {
    std::lock_guard<std::mutex> lck(mu);
    LOG_INFO("clear, rebind");
    done = false;
    cpu_id = 0;
    threadid_set.clear();
    return true;
  }

 private:
  bool done;  // 当16个线程全部绑核完成之后，置为true
  std::atomic<int> cpu_id;
  std::mutex mu;
  std::set<uint64_t> threadid_set;
};

}  // namespace kv