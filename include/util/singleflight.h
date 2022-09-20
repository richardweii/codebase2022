#ifndef _SINGLEFLIGHT_H_
#define _SINGLEFLIGHT_H_

#include <memory>
#include <unordered_map>
#include "util/rwlock.h"

namespace kv {
constexpr int kSingleFlightSharding = 16;

template <typename _Key, typename _Val>
class SingleFlight {
 public:
  template <class Func, typename... Args>
  _Val Do(const _Key &key, uint32_t hash, Func &&func, Args &&...args) {
    uint32_t shard = hash % kSingleFlightSharding;
    SpinLock &lock = _lock[shard];
    TaskTable &tt = _do[shard];
    // whether someone is executing
    auto iter = tt.find(key);
    if (iter != tt.end()) {
      std::shared_ptr<_Result> pRes = iter->second;
      lock.Unlock();
      // waiting...
      while (!pRes->_done)
        ;
      // return result
      return pRes->_result;
    }
    // create new task
    std::shared_ptr<_Result> pRes = std::make_shared<_Result>();
    pRes->_done = false;
    tt[key] = pRes;
    lock.Unlock();

    // do work...
    pRes->_result = func(std::forward<Args>(args)...);
    pRes->_done = true;

    // delete from tasking list
    lock.Lock();
    tt.erase(key);
    lock.Unlock();
    return pRes->_result;
  }

 private:
  struct _Result {
    volatile bool _done;
    _Val _result;
  };
  using TaskTable = std::unordered_map<_Key, std::shared_ptr<_Result>>;
  SpinLock _lock[kSingleFlightSharding];
  TaskTable _do[kSingleFlightSharding];
};
}  // namespace kv

#endif  // _SINGLEFLIGHT_H_