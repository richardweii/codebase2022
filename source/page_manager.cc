#include "page_manager.h"

namespace kv {

PageManager *global_page_manager = nullptr;

// TODO: 可以考虑将_free_list拆成16个，这样就可以分配的时候多线程同时分配了
// 1. 需要为pagemeta增加一个字段free_list_id，代表这个_page是从哪个队列拿的，到时候free的时候物归原主
// 2. 需要更改BusyBits，找到一个为0的bit位之后，需要CAS操作
// 为了性能，最好自己实现一个并发的ffl
// 或者说随机选一个位，感觉性能也许会更好
PageManager::PageManager(size_t page_num) : _page_num(page_num) {
  _pages = new PageMeta[page_num];
  for (size_t i = 0; i < page_num - 1; i++) {
    _pages[i]._next = &_pages[i + 1];
    _pages[i]._page_id = i;
  }
  _pages[page_num - 1]._page_id = page_num - 1;
  _free_page_num = page_num;
  _free_list = &_pages[0];
}

PageManager::~PageManager() {
  for (size_t i = 0; i < _page_num; i++) {
    DeleteBitmap(_pages[i]._bitmap);
  }
  delete[] _pages;
}

PageMeta *PageManager::AllocNewPage(uint8_t slab_class) {
  _lock.Lock();
  defer { _lock.Unlock(); };
  if (_free_list == nullptr) {
    LOG_ERROR("page used up.");
    return nullptr;
  }
  PageMeta *res = _free_list;
  _free_list = _free_list->_next;
  // LOG_DEBUG("alloc new page for class %d", slab_class);
  res->reset(NewBitmap(kPageSize / kSlabSize / slab_class));
  res->_slab_class = slab_class;
  _free_page_num--;
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
  _free_page_num++;
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
}

void PageManager::Mount(PageMeta **list_tail, PageMeta *meta) {
  assert(list_tail != nullptr);
  assert(meta != nullptr);
  if (meta != *list_tail && meta->_prev == nullptr && meta->_next == nullptr) {
    // LOG_DEBUG("mount page %d", meta->_page_id);
    (*list_tail)->_next = meta;
    meta->_prev = (*list_tail);
    meta->_next = nullptr;
    (*list_tail) = meta;
  }
}

}  // namespace kv