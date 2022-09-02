#pragma once

#include <infiniband/verbs.h>
#include <cstddef>
#include <list>
#include <vector>
#include "config.h"
#include "rdma_client.h"
#include "util/rwlock.h"

namespace kv {

class FrameHashTable;
class ClockReplacer;

struct PageData {
  char data[kPageSize];
};

class PageEntry {
  friend class BufferPool;

 public:
  bool Dirty = false;
  uint8_t SlabClass = 0;
  kv::PageId PageId() const { return _page_id; }
  char *Data() { return _data->data; }

 private:
  kv::PageId _page_id = INVALID_PAGE_ID;
  PageData *_data;
  FrameId _frame_id = INVALID_FRAME_ID;
  // TODO: maybe need a latch
};

class BufferPool {
 public:
  BufferPool(size_t buffer_pool_size);
  ~BufferPool();

  bool Init(ibv_pd *pd);

  // return nullptr if no free page
  PageEntry *FetchNew(PageId page_id);

  PageEntry *Lookup(PageId addr);

  void Release(PageEntry *entry);

  // used with evict
  void InsertPage(PageEntry *page, PageId page_id);

  PageEntry *Evict();
  PageEntry *EvictBatch(int batch_size, std::vector<PageEntry *> *pages = nullptr);

  // prevent from evicting
  void PinPage(PageEntry *entry);
  void UnpinPage(PageEntry *entry);

  ibv_mr *MR() const { return _mr; }

 private:
  PageData *_pages;
  PageEntry *_entries;

  ibv_mr *_mr = nullptr;
  ClockReplacer *_replacer;
  FrameHashTable *_hash_table;
  size_t _buffer_pool_size;
  SpinLatch _latch;
};
}  // namespace kv