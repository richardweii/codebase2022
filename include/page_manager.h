#pragma once

#include <cstddef>
#include <list>
#include "config.h"
#include "util/arena.h"
#include "util/bitmap.h"
#include "util/rwlock.h"

namespace kv {

class PageMeta {
 public:
  friend class PageManager;

  int SetFirstFreePos() {
    return _bitmap->get_free();
  }

  void ClearPos(int idx) {
    _bitmap->put_back(idx);
  }

  bool Full() const { return _bitmap->Full(); }

  bool Empty() const { return _bitmap->Empty(); }

  kv::PageId PageId() const { return _page_id; }

  uint8_t SlabClass() const { return _slab_class; }

  PageMeta *Next() const { return _next; }
  PageMeta *Prev() const { return _prev; }

  bool IsPined() const { return pin_; }
  void Pin() { pin_ = true; }
  void UnPin() { pin_ = false; }

 private:
  void reset(Bitmap *bitmap) {
    DeleteBitmap(_bitmap);
    _bitmap = bitmap;
    _next = nullptr;
    _prev = nullptr;
    pin_ = false;
  }
  Bitmap *_bitmap = nullptr;
  PageMeta *_next = nullptr;
  PageMeta *_prev = nullptr;
  uint32_t _page_id;
  uint8_t _slab_class;
  bool pin_;
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
  size_t _page_num = 0;
  SpinLock _lock;
};

extern PageManager *global_page_manager;
}  // namespace kv
