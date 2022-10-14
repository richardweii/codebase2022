#pragma once

#include <infiniband/verbs.h>
#include <cstddef>
#include <cstdint>
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
  kv::PageId PageId() const { return _page_id; }
  uint8_t MRID() const { return mr_id; }
  char *Data() { return _data->data; }
  uint8_t SlabClass() const { return _slab_class; }

  void SetComSize(size_t sz) {
    com_sz = sz;
  }

  size_t GetComSize() const {
    return com_sz;
  }

 private:
  kv::PageId _page_id = INVALID_PAGE_ID;
  PageData *_data;
  uint8_t _slab_class = 0;
  FrameId _frame_id = INVALID_FRAME_ID;
  bool _writer = false;
  uint8_t mr_id;
  size_t com_sz;
};

class BufferPool {
 public:
  BufferPool(size_t buffer_pool_size, uint8_t shard);
  ~BufferPool();

  bool Init(ibv_pd *pd);

  // return nullptr if no free page
  PageEntry *FetchNew(PageId page_id, uint8_t slab_class);

  PageEntry *Lookup(PageId page_id, bool writer = false);

  void Release(PageEntry *entry);
  // used with evict
  void InsertPage(PageEntry *page, PageId page_id, uint8_t slab_class);

  PageEntry *Evict();
  PageEntry *EvictBatch(int batch_size, std::vector<PageEntry *> *pages = nullptr);

  // prevent from evicting
  void PinPage(PageEntry *entry);
  void UnpinPage(PageEntry *entry);

  // ibv_mr *MR() const { return _mr; }
  // ibv_mr *MR(int id) const { return _mr[id]; }

  ibv_mr *CompressMR() const { return compress_page_buff_mr; }
  PageData compress_page_buff[kThreadNum];

 private:
  std::atomic_int pin{0};
  PageData *_pages;
  PageEntry *_entries;

  // ibv_mr *_mr[4];
  ClockReplacer *_replacer;
  FrameHashTable *_hash_table;
  size_t _buffer_pool_size;
  uint8_t _shard;
  SpinLatch _latch;
  // for compress
  ibv_mr *compress_page_buff_mr;
};
}  // namespace kv