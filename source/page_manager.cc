#include "page_manager.h"
#include "util/logging.h"

namespace kv {

PageManager *global_page_manager = nullptr;
PageManager::PageManager(size_t page_num) : _page_num(page_num) {
  _pages = new PageMeta[page_num];
  for (size_t i = 0; i < page_num - 1; i++) {
    _pages[i]._next = &_pages[i + 1];
    _pages[i]._page_id = i;
  }
  _pages[page_num - 1]._page_id = page_num - 1;
  _free_list = &_pages[0];
}

PageManager::~PageManager() {
  for (size_t i = 0; i < _page_num; i++) {
    DeleteBitmap(_pages[i]._bitmap);
  }
  delete[] _pages;
}

#define CAS(_p, _u, _v) (__atomic_compare_exchange_n(_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))
PageMeta *PageManager::AllocNewPage(uint8_t slab_class) {
  _lock.Lock();

  if (_free_list == nullptr) {
    _lock.Unlock();
    LOG_ERROR("page used up.");
    return nullptr;
  }
  PageMeta *res = _free_list;
  _free_list = _free_list->_next;
  _lock.Unlock();
  // LOG_DEBUG("alloc new page for class %d", slab_class);
  res->reset(NewBitmap(kPageSize / kSlabSize / slab_class));
  res->_slab_class = slab_class;
  return res;
}

void PageManager::FreePage(uint32_t page_id) {
  // LOG_DEBUG("free page %d", page_id);
  LOG_ASSERT(page_id < _page_num, "page_id %d out of range %ld", page_id, _page_num);
  _lock.Lock();
  defer { _lock.Unlock(); };
  PageMeta *meta = &_pages[page_id];
  meta->_prev = nullptr;
  meta->_next = _free_list;
  _free_list = meta;
}

void PageManager::Unmount(PageMeta *meta) {
  // LOG_DEBUG("unmount page %d", meta->_page_id);
  if (meta->_prev) {
    meta->_prev->_next = meta->_next;
    if (meta->_next) {
      meta->_next->_prev = meta->_prev;
    }
  } else if (meta->_next) {
    // head
    meta->_next->_prev = nullptr;
  }
  meta->_prev = nullptr;
  meta->_next = nullptr;
  meta->mounted = false;
}

void PageManager::Mount(PageMeta **list_tail, PageMeta *meta) {
  assert(list_tail != nullptr);
  assert(meta != nullptr);
  if (!meta->IsMounted() && !meta->IsPined() && !meta->Full() && !meta->Empty() && meta != *list_tail && meta->_prev == nullptr && meta->_next == nullptr) {
    meta->mounted = true;
    // LOG_DEBUG("mount page %d", meta->_page_id);
    LOG_ASSERT(*list_tail != nullptr, "allocating page should not be null");
    meta->_prev = (*list_tail);
    meta->_next = nullptr;
    (*list_tail)->_next = meta;
    (*list_tail) = meta;
    LOG_ASSERT(meta->_prev != nullptr, "allocating page should not be null");
    LOG_ASSERT(!meta->Full(), "meta is not full");
    LOG_ASSERT(!meta->IsPined(), "meta is pined");
  }
}

}  // namespace kv