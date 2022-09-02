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
    _used++;
    return _bitmap->SetFirstFreePos();
  }

  void ClearPos(int idx) {
    _used--;
    _bitmap->Clear(idx);
  }

  bool Full() const { return _used == _cap; }

  bool Empty() const { return _used == 0; }

  uint32_t PageId() const { return _page_id; }

  uint8_t SlabClass() const { return _slab_class; }

 private:
  void reset(Bitmap *bitmap) {
    DeleteBitmap(_bitmap);
    _bitmap = bitmap;
    _used = 0;
    _cap = bitmap->Cap();
  }
  Bitmap *_bitmap = nullptr;
  // PageMeta *_next = nullptr;  // TODO: lock-free linked list
  uint32_t _page_id;
  uint16_t _cap = 0;
  uint16_t _used = 0;
  uint8_t _slab_class;
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

 private:
  PageMeta *_pages = nullptr;
  std::list<PageMeta *> _free_list;
  int _free_page_num = 0;
  size_t _page_num = 0;
  SpinLock _lock;
};
}  // namespace kv
