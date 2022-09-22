#ifndef _SINGLEFLIGHT_H_
#define _SINGLEFLIGHT_H_

#include <memory>
#include <unordered_map>
#include "buffer_pool.h"
#include "config.h"
#include "util/logging.h"
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
    lock.Lock();
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

// 特化一下
template <>
class SingleFlight<PageId, PageEntry *> {
 public:
  PageEntry *Do(const PageId &key, uint32_t hash, std::function<PageEntry *(PageId, uint8_t, bool)> &func,
                PageId page_id, uint8_t slab_class, bool writer) {
    uint32_t shard = hash % kSingleFlightSharding;
    SpinLock &lock = _lock[shard];
    TaskTable &tt = _do[shard];
    lock.Lock();
    // whether someone is executing
    auto iter = tt.find(key);
    if (iter != tt.end()) {
      std::shared_ptr<_Result> pRes = iter->second;
      lock.Unlock();
      // waiting...
      while (!pRes->_done)
        ;
      // return result
      // if (writer) {
      //   pRes->_result->WLock();
      // } else {
      //   pRes->_result->RLock();
      // }
      return pRes->_result;
    }
    // create new task
    std::shared_ptr<_Result> pRes = std::make_shared<_Result>();
    pRes->_done = false;
    tt[key] = pRes;
    lock.Unlock();

    // do work...
    pRes->_result = func(page_id, slab_class, writer);
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
    PageEntry *_result;
  };
  using TaskTable = std::unordered_map<PageId, std::shared_ptr<_Result>>;
  SpinLock _lock[kSingleFlightSharding];
  TaskTable _do[kSingleFlightSharding];
};
}  // namespace kv

#endif  // _SINGLEFLIGHT_H_