#pragma once

#include <cstddef>
#include <cstdint>
#include <list>
#include "config.h"
#include "util/arena.h"
#include "util/bitmap.h"
#include "util/logging.h"
#include "util/rwlock.h"

namespace kv {

class PageMeta {
 public:
  friend class PageManager;

  int SetFirstFreePos() {
    _used++;
    return _bitmap->SetFirstFreePos(_index_bitmap);
  }

  void ClearPos(int idx) {
    _used--;
    _bitmap->Clear(idx, _index_bitmap);
  }

  bool Full() const { return (_used == _cap); }

  bool Empty() const { return _used == 0; }

  bool IsMounted() const { return mounted; }

  kv::PageId PageId() const { return _page_id; }

  uint8_t SlabClass() const { return _slab_class; }

  PageMeta *Next() const { return _next; }
  PageMeta *Prev() const { return _prev; }

  bool IsPined() const { return pin_; }
  void Pin() {
    LOG_ASSERT(!pin_, "pined");
    pin_ = true;
    mounted = true;
  }
  void UnPin() {
    LOG_ASSERT(pin_, "unpined");
    pin_ = false;
    mounted = false;
  }
  void Lock() { lock_.Lock(); }
  void Unlock() { lock_.Unlock(); }
  int8_t al_index = -1;

 private:
  void reset(Bitmap *bitmap) {
    DeleteBitmap(_bitmap);
    DeleteBitmap(_index_bitmap);
    _bitmap = bitmap;
    _index_bitmap = NewBitmap(bitmap->Size());
    _used = 0;
    _cap = bitmap->Cap();
    _next = nullptr;
    _prev = nullptr;
  }
  std::atomic<bool> mounted;
  bool pin_;
  uint8_t _slab_class;
  uint16_t _cap = 0;
  uint16_t _used = 0;
  uint32_t _page_id;
  Bitmap *_bitmap = nullptr;
  Bitmap *_index_bitmap = nullptr;
  PageMeta *_next = nullptr;
  PageMeta *_prev = nullptr;
  SpinLock lock_;
};

/**
 * @brief Used to manage page meta data
 *
 */
class PageManager {
 public:
  PageManager(size_t page_num);

  ~PageManager();

  // thread-safe
  PageMeta *Page(uint32_t page_id) {
    const int per_thread_page_num = _page_num/kThreadNum;
    LOG_ASSERT(page_id < _page_num, "page_id %u out of range %lu", page_id, _page_num);
    int tid = page_id / per_thread_page_num;
    int off = page_id % per_thread_page_num;
    // LOG_INFO("_page_num %ld page_id %d tid %d off %d", _page_num, page_id, tid, off);
    return &_pages[tid][off];
  }

  // mutex
  PageMeta *AllocNewPage(uint8_t slab_class, int tid);
  // mutex
  void FreePage(uint32_t page_id);

  // unmount a page from allocing list
  void Unmount(PageMeta *meta);

  // mount a page to allocing list
  void Mount(PageMeta **list_tail, PageMeta *meta);

 private:
  PageMeta *_pages[kThreadNum];
  PageMeta *_free_list[kThreadNum];
  size_t _page_num = 0;
  SpinLock _lock;
};

extern PageManager *global_page_manager;
}  // namespace kv
