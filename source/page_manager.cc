#include "page_manager.h"
#include "util/logging.h"

namespace kv {

PageManager *global_page_manager = nullptr;
PageManager::PageManager(size_t page_num) : _page_num(page_num) {
  for (int i = 0; i < kThreadNum; i++) {
    _pages[i] = new PageMeta[page_num / kThreadNum];
  }

  const int per_thread_page_num = page_num / kThreadNum;
  for (int i = 0; i < kThreadNum; i++) {
    for (int j = 0; j < per_thread_page_num - 1; j++) {
      _pages[i][j]._next = &_pages[i][j + 1];
      _pages[i][j]._page_id = i * per_thread_page_num + j;
    }
    _pages[i][per_thread_page_num - 1]._page_id = (i - 1) * per_thread_page_num;
    _free_list[i] = &_pages[i][0];
  }
}

PageManager::~PageManager() {
  for (int t = 0; t < kThreadNum; t++) {
    for (size_t i = 0; i < _page_num; i++) {
      DeleteBitmap(_pages[t][i]._bitmap);
    }
    delete[] _pages[t];
  }
}

PageMeta *PageManager::AllocNewPage(uint8_t slab_class, int tid) {
  if (_free_list[tid] == nullptr) {
    LOG_ASSERT(false, "page used up.");
    return nullptr;
  }
  PageMeta *res = _free_list[tid];
  _free_list[tid] = _free_list[tid]->_next;
  // LOG_DEBUG("alloc new page for class %d", slab_class);
  res->reset(NewBitmap(kPageSize / kSlabSize / slab_class));
  res->_slab_class = slab_class;
  return res;
}

void PageManager::FreePage(uint32_t page_id) {
  // LOG_DEBUG("free page %d", page_id);
  LOG_ASSERT(page_id < _page_num, "page_id %d out of range %ld", page_id, _page_num);
  const int per_thread_page_num = _page_num / kThreadNum;
  int tid = page_id / per_thread_page_num;
  int off = page_id % per_thread_page_num;
  PageMeta *meta = &_pages[tid][off];
  meta->_prev = nullptr;
  meta->_next = _free_list[cur_thread_id];
  meta->mounted = false;
  meta->pin_ = false;
  _free_list[cur_thread_id] = meta;
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
  if (!meta->IsMounted() && !meta->IsPined() && !meta->Full() && meta != *list_tail && meta->_prev == nullptr &&
      meta->_next == nullptr) {
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