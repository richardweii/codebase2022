#include "page_manager.h"

namespace kv {

PageManager::PageManager(size_t page_num, uint8_t shard) : _page_num(page_num), _shard(shard) {
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
  if (_free_list == nullptr) {
    LOG_ERROR("page used up.");
    return nullptr;
  }
  PageMeta *res = _free_list;
  _free_list = _free_list->_next;
  LOG_DEBUG("[shard %d] alloc new page for class %d", _shard, slab_class);
  res->reset(NewBitmap(kPageSize / kSlabSize / slab_class));
  res->_slab_class = slab_class;
  _free_page_num--;
  return res;
}

void PageManager::FreePage(uint32_t page_id) {
  LOG_DEBUG("[shard %d] free page %d", _shard, page_id);
  LOG_ASSERT(page_id < _page_num, "page_id %d out of range %ld", page_id, _page_num);
  PageMeta *meta = &_pages[page_id];
  meta->_prev = nullptr;
  meta->_next = _free_list;
  _free_list = meta;
  _free_page_num++;
}

void PageManager::Unmount(PageMeta *meta) {
  LOG_DEBUG("[shard %d] unmount page %d", _shard, meta->_page_id);
  if (meta->_prev) {
    meta->_prev->_next = meta->_next;
    if (meta->_next) {
      meta->_next->_prev = meta->_prev;
    }
  }
  meta->_prev = nullptr;
  meta->_next = nullptr;
}

void PageManager::Mount(PageMeta **list_tail, PageMeta *meta) {
  assert(list_tail != nullptr);
  assert(meta != nullptr);
  if (meta != *list_tail && meta->_prev == nullptr && meta->_next == nullptr) {
    LOG_DEBUG("[shard %d] mount page %d", _shard, meta->_page_id);
    (*list_tail)->_next = meta;
    meta->_prev = (*list_tail);
    meta->_next = nullptr;
    (*list_tail) = meta;
  }
}

}  // namespace kv