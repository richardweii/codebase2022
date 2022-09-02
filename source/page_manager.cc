#include "page_manager.h"

namespace kv {

PageManager::PageManager(size_t page_num) : _page_num(page_num) {
  _pages = new PageMeta[page_num];
  for (size_t i = 0; i < page_num; i++) {
    // _pages[i]._next = &_pages[i + 1];
    _pages[i]._page_id = i;
    _free_list.push_back(&_pages[i]);
  }
  _free_page_num = page_num;
}

PageManager::~PageManager() {
  for (size_t i = 0; i < _page_num; i++) {
    DeleteBitmap(_pages[i]._bitmap);
  }
  delete[] _pages;
}

PageMeta *PageManager::AllocNewPage(uint8_t slab_class) {
  if (_free_list.empty()) {
    LOG_ERROR("page used up.");
    return nullptr;
  }
  PageMeta *res = _free_list.front();
  _free_list.pop_front();
  res->reset(NewBitmap(kPageSize / kSlabSize / slab_class));
  res->_slab_class = slab_class;
  _free_page_num--;
  return res;
}

void PageManager::FreePage(uint32_t page_id) {
  // if (page_id == 3256) {
  //   LOG_INFO("free 3256");
  // }
  LOG_ASSERT(page_id < _page_num, "page_id %d out of range %ld", page_id, _page_num);
  PageMeta *meta = &_pages[page_id];
  _free_list.push_back(meta);
  _free_page_num++;
}
}  // namespace kv