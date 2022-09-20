#pragma once

#include <cstddef>
#include <list>
#include "config.h"
#include "util/arena.h"
#include "util/bitmap.h"

namespace kv {

class PageMeta {
 public:
  friend class PageManager;

  int SetFirstFreePos() {
    _latch.WLock();
    _used++;
    int pos = _bitmap->SetFirstFreePos();
    _latch.WUnlock();
    return pos;
  }

  void ClearPos(int idx) {
    _latch.WLock();
    _used--;
    _bitmap->Clear(idx);
    _latch.WUnlock();
  }

  bool Full() const {
    return (_used == _cap);
  }

  bool Empty() const {
    return _used == 0;
  }

  kv::PageId PageId() const { return _page_id; }

  uint8_t SlabClass() const { return _slab_class; }

  PageMeta *Next() const { return _next; }
  PageMeta *Prev() const { return _prev; }

 private:
  void reset(Bitmap *bitmap) {
    DeleteBitmap(_bitmap);
    _bitmap = bitmap;
    _used = 0;
    _cap = bitmap->Cap();
    _next = nullptr;
    _prev = nullptr;
  }
  Bitmap *_bitmap = nullptr;
  PageMeta *_next = nullptr;
  PageMeta *_prev = nullptr;
  uint32_t _page_id;
  uint16_t _cap = 0;
  uint16_t _used = 0;
  uint8_t _slab_class;
  mutable SpinLatch _latch;
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
    LOG_ASSERT(page_id < _page_num, "page_id %u out of range %lu", page_id, _page_num);
    return &_pages[page_id];
  }

  // mutex
  PageMeta *AllocNewPage(uint8_t slab_class);
  // mutex
  void FreePage(uint32_t page_id);

  // unmount a page from allocing list
  void Unmount(PageMeta *meta);

  // mount a page to allocing list
  void Mount(PageMeta **list_tail, PageMeta *meta);

 private:
  PageMeta *_pages = nullptr;
  PageMeta *_free_list;
  int _free_page_num = 0;
  size_t _page_num = 0;
  SpinLock _lock;
};

extern PageManager *global_page_manger;
}  // namespace kv
