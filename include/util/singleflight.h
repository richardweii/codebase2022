#ifndef _SINGLEFLIGHT_H_
#define _SINGLEFLIGHT_H_

#include <memory>
#include <unordered_map>
#include "buffer_pool.h"
#include "config.h"
#include "util/logging.h"
#include "util/rwlock.h"

namespace kv {

template <typename _Key, typename _Val>
class SingleFlight {
 public:
  template <class Func, typename... Args>
  _Val Do(const _Key &key, uint32_t hash, Func &&func, Args &&...args) {
    page_locks_[hash].Lock();
    if (_do[hash] != nullptr) {
      std::shared_ptr<_Result> pRes = _do[hash];
      page_locks_[hash].Unlock();
      // waiting...
      while (!pRes->_done)
        ;
      // return result
      return pRes->_result;
    }
    // create new task
    std::shared_ptr<_Result> pRes = std::make_shared<_Result>();
    pRes->_done = false;
    _do[hash] = pRes;
    page_locks_[hash].Unlock();

    // do work...
    pRes->_result = func(std::forward<Args>(args)...);
    pRes->_done = true;

    // delete from tasking list
    page_locks_[hash].Lock();
    _do[hash] = nullptr;
    page_locks_[hash].Unlock();
    return pRes->_result;
  }
};

// 特化一下
template <>
class SingleFlight<PageId, PageEntry *> {
 public:
  PageEntry *Do(const PageId &key, uint32_t hash, std::function<PageEntry *(PageId, uint8_t, bool)> &func,
                PageId page_id, uint8_t slab_class, bool writer) {
    page_locks_[hash].Lock();
    if (_do[hash] != nullptr) {
      std::shared_ptr<_Result> pRes = _do[hash];
      page_locks_[hash].Unlock();
      // waiting...
      while (!pRes->_done)
        ;
      // return result
      // if (writer) {
      //   pRes->_result->WLock();
      // } else {
      //   pRes->_result->RLock();
      // }
      // return result
      return pRes->_result;
    }

    // create new task
    std::shared_ptr<_Result> pRes = std::make_shared<_Result>();
    pRes->_done = false;
    _do[hash] = pRes;
    page_locks_[hash].Unlock();

    // do work...
    pRes->_result = func(page_id, slab_class, writer);
    pRes->_done = true;

    // delete from tasking list
    page_locks_[hash].Lock();
    _do[hash] = nullptr;
    page_locks_[hash].Unlock();
    return pRes->_result;
  }
};
}  // namespace kv

#endif  // _SINGLEFLIGHT_H_