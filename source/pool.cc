#include "pool.h"
#include "config.h"
#include "stat.h"
#include "util/likely.h"
#include "util/logging.h"
#include "util/memcpy.h"

namespace kv {

Pool::Pool(uint8_t shard, RDMAClient *client, std::vector<MemoryAccess> *global_rdma_access)
    : _access_table(global_rdma_access), _client(client), _shard(shard) {
  _buffer_pool = new BufferPool(kBufferPoolSize / kPoolShardingNum, shard);
  _hash_index = new HashTable(kKeyNum / kPoolShardingNum);
  _replacement =
      std::bind(&Pool::replacement, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
  _writeNew = std::bind(&Pool::writeNew, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
}

Pool::~Pool() {
  delete _buffer_pool;
  delete _hash_index;
}

void Pool::Init() {
  auto succ = _buffer_pool->Init(_client->Pd());
  LOG_ASSERT(succ, "Init buffer pool failed.");

  for (int i = kSlabSizeMin; i <= 16; i++) {
    for (int j = 0; j < kAllocingListShard; j++) {
      PageMeta *page = global_page_manager->AllocNewPage(i);
      _small_allocing_tail[j][i] = page;
      _small_allocing_pages[j][i] = _buffer_pool->FetchNew(page->PageId(), i);
      _buffer_pool->PinPage(_small_allocing_pages[j][i]);
      _small_allocing_tail[j][i]->Pin();
    }
  }

  for (int i = 17; i <= kSlabSizeMax; i++) {
    PageMeta *page = global_page_manager->AllocNewPage(i);
    _big_allocing_tail[i] = page;
    _big_allocing_pages[i] = _buffer_pool->FetchNew(page->PageId(), i);
    _buffer_pool->PinPage(_big_allocing_pages[i]);
    _big_allocing_tail[i]->Pin();
  }
}

bool Pool::Read(const Slice &key, uint32_t hash, std::string &val) {
  // existence
  KeySlot *slot = _hash_index->Find(key, hash);
  if (UNLIKELY(slot == nullptr)) {
#ifdef STAT
    stat::read_miss.fetch_add(1);
#endif
    return false;
  }
  Addr addr = slot->Addr();
  PageId page_id = AddrParser::PageId(addr);
  PageMeta *meta = global_page_manager->Page(page_id);
  // cache
  PageEntry *entry = _buffer_pool->Lookup(page_id);

  if (LIKELY(entry != nullptr)) {
#ifdef STAT
    stat::cache_hit.fetch_add(1, std::memory_order_relaxed);
#endif
    uint32_t val_len = entry->SlabClass() * kSlabSize;
    val.resize(val_len);
    my_memcpy((char *)val.data(), entry->Data() + val_len * AddrParser::Off(addr), val_len);
    _buffer_pool->Release(entry);
    return true;
  }

  // cache miss
  // page_locks_[page_id].Lock();
  // PageEntry *victim = replacement(page_id, meta->SlabClass(), false);
  PageEntry *victim = _replacement_sgfl.Do(page_id, page_id, _replacement, page_id, meta->SlabClass(), false);
  // page_locks_[page_id].Unlock();
  uint32_t val_len = victim->SlabClass() * kSlabSize;
  val.resize(val_len);
  my_memcpy((char *)val.data(), victim->Data() + val_len * AddrParser::Off(addr), val_len);

  _buffer_pool->Release(victim);
  return true;
}

bool Pool::Write(const Slice &key, uint32_t hash, const Slice &val) {
  KeySlot *slot = _hash_index->Find(key, hash);
  if (slot == nullptr) {
    // insert new KV
    writeNew(key, hash, val);
    return true;
  }

  Addr addr = slot->Addr();
  PageMeta *meta = global_page_manager->Page(AddrParser::PageId(addr));
  PageId page_id = meta->PageId();

  // modify in place
  if (LIKELY(meta->SlabClass() == val.size() / kSlabSize)) {
    PageEntry *entry = _buffer_pool->Lookup(page_id, true);
    if (entry != nullptr) {
#ifdef STAT
      stat::cache_hit.fetch_add(1, std::memory_order_relaxed);
#endif
      if (!entry->Dirty) entry->Dirty = true;

      my_memcpy((char *)(entry->Data() + val.size() * AddrParser::Off(addr)), val.data(), val.size());
      _buffer_pool->Release(entry);
      return true;
    }

    // cache miss
    PageEntry *victim = _replacement_sgfl.Do(page_id, page_id, _replacement, page_id, meta->SlabClass(), true);
    my_memcpy((char *)(victim->Data() + val.size() * AddrParser::Off(addr)), val.data(), val.size());
    if (!victim->Dirty) victim->Dirty = true;

    _buffer_pool->Release(victim);
    return true;
  }

  modifyLength(slot, val, hash);
  return true;
}

bool Pool::Delete(const Slice &key, uint32_t hash) {
  KeySlot *slot = _hash_index->Remove(key, hash);
  if (slot == nullptr) {
    LOG_ERROR("delete invalid file %s", key.data());
    return false;
  }

  Addr addr = slot->Addr();
  PageId page_id = AddrParser::PageId(addr);
  PageMeta *meta = global_page_manager->Page(page_id);

  int al_index = (hash >> 24) % kAllocingListShard;
  meta->ClearPos(AddrParser::Off(addr));
  allocingListWLock(al_index, meta->SlabClass());
  if (isSmallSlabSize(meta->SlabClass())) {
    global_page_manager->Mount(&_small_allocing_tail[al_index][meta->SlabClass()], meta);
    if (meta->Empty() && _small_allocing_pages[al_index][meta->SlabClass()]->PageId() != page_id) {
      if (_small_allocing_tail[al_index][meta->SlabClass()]->PageId() == page_id) {
        _small_allocing_tail[al_index][meta->SlabClass()] = meta->Prev();
        LOG_ASSERT(_small_allocing_tail[al_index][meta->SlabClass()] != nullptr, "tail should not be null");
      }
      global_page_manager->Unmount(meta);
      allocingListWUnlock(al_index, meta->SlabClass());
      global_page_manager->FreePage(page_id);
    } else {
      allocingListWUnlock(al_index, meta->SlabClass());
    }
  } else {
    global_page_manager->Mount(&_big_allocing_tail[meta->SlabClass()], meta);
    if (meta->Empty() && _big_allocing_pages[meta->SlabClass()]->PageId() != page_id) {
      if (_big_allocing_tail[meta->SlabClass()]->PageId() == page_id) {
        _big_allocing_tail[meta->SlabClass()] = meta->Prev();
      }
      global_page_manager->Unmount(meta);
      allocingListWUnlock(al_index, meta->SlabClass());
      global_page_manager->FreePage(page_id);
    } else {
      allocingListWUnlock(al_index, meta->SlabClass());
    }
  }

  _hash_index->GetSlotMonitor()->FreeSlot(slot);
  return true;
}

void Pool::modifyLength(KeySlot *slot, const Slice &val, uint32_t hash) {
  // delete
  Addr addr = slot->Addr();
  PageId page_id = AddrParser::PageId(addr);
  PageMeta *meta = global_page_manager->Page(page_id);
  assert(meta->SlabClass() != val.size() / kSlabSize);

  int al_index = (hash >> 24) % kAllocingListShard;
  meta->ClearPos(AddrParser::Off(addr));
  allocingListWLock(al_index, meta->SlabClass());
  if (LIKELY(isSmallSlabSize(meta->SlabClass()))) {
    global_page_manager->Mount(&_small_allocing_tail[al_index][meta->SlabClass()], meta);
    if (meta->Empty() && _small_allocing_pages[al_index][meta->SlabClass()]->PageId() != page_id) {
      if (_small_allocing_tail[al_index][meta->SlabClass()]->PageId() == page_id) {
        _small_allocing_tail[al_index][meta->SlabClass()] = meta->Prev();
        LOG_ASSERT(_small_allocing_tail[al_index][meta->SlabClass()] != nullptr, "tail should not be null");
        // LOG_DEBUG("[shard %d] set class %d tail page %d", _shard, meta->SlabClass(), meta->Prev()->PageId());
      }
      global_page_manager->Unmount(meta);
      allocingListWUnlock(al_index, meta->SlabClass());
      global_page_manager->FreePage(page_id);
    } else {
      allocingListWUnlock(al_index, meta->SlabClass());
    }
  } else {
    global_page_manager->Mount(&_big_allocing_tail[meta->SlabClass()], meta);
    if (meta->Empty() && _big_allocing_pages[meta->SlabClass()]->PageId() != page_id) {
      if (_big_allocing_tail[meta->SlabClass()]->PageId() == page_id) {
        _big_allocing_tail[meta->SlabClass()] = meta->Prev();
        // LOG_DEBUG("[shard %d] set class %d tail page %d", _shard, meta->SlabClass(), meta->Prev()->PageId());
      }
      global_page_manager->Unmount(meta);
      allocingListWUnlock(al_index, meta->SlabClass());
      global_page_manager->FreePage(page_id);
    } else {
      allocingListWUnlock(al_index, meta->SlabClass());
    }
  }

  // rewrite to meta
  uint8_t slab_class = val.size() / kSlabSize;

  PageEntry *page;
  RDMAManager::Batch *batch = nullptr;
  int off;
  // if (!isSmallSlabSize(slab_class)) {
  //   _big_allocing_list_latch[slab_class].WLock();
  // }
  allocingListWLock(al_index, slab_class);
  if (LIKELY(isSmallSlabSize(slab_class))) {
    page = _small_allocing_pages[al_index][slab_class];
    meta = global_page_manager->Page(page->PageId());
    if (UNLIKELY(meta->Full())) {
      page = mountNewPage(slab_class, hash, &batch, -1);
      meta = global_page_manager->Page(page->PageId());
    }
    LOG_ASSERT(!meta->Full(), "meta should not full");
    // modify bitmap
    off = meta->SetFirstFreePos();
  } else {
    page = _big_allocing_pages[slab_class];
    meta = global_page_manager->Page(page->PageId());
    if (UNLIKELY(meta->Full())) {
      page = mountNewPage(slab_class, hash, &batch, -1);
      meta = global_page_manager->Page(page->PageId());
    }
    LOG_ASSERT(!meta->Full(), "meta should not full");
    // modify bitmap
    off = meta->SetFirstFreePos();
  }
  if (batch != nullptr) {
    batch->FinishBatch();
    delete batch;
  }
  // if (!isSmallSlabSize(slab_class)) {
  //   _big_allocing_list_latch[slab_class].WUnlock();
  // }
  allocingListWUnlock(al_index, slab_class);

  my_memcpy((char *)(page->Data() + val.size() * off), val.data(), val.size());
  if (!page->Dirty) page->Dirty = true;
  _buffer_pool->Release(page);

  LOG_ASSERT(off != -1, "set bitmap failed.");
  addr = AddrParser::GenAddrFrom(meta->PageId(), off);
  slot->SetAddr(addr);
}

// 这个函数if else是相同的逻辑，只是调用的成员对象不同，懒得复用代码，看的时候只看一个分支就行
PageEntry *Pool::mountNewPage(uint8_t slab_class, uint32_t hash, RDMAManager::Batch **batch_ret, int tid) {
  int al_index;
  if (tid == -1) {
    al_index = (hash >> 24) % kAllocingListShard;
  } else {
    al_index = tid;
  }

  if (LIKELY(isSmallSlabSize(slab_class))) {
    PageEntry *old_entry = _small_allocing_pages[al_index][slab_class];
    PageMeta *old_meta = global_page_manager->Page(old_entry->PageId());
    _buffer_pool->UnpinPage(old_entry);
    old_meta->UnPin();

    _small_allocing_pages[al_index][slab_class] = nullptr;

    PageMeta *meta = old_meta->Next();
    PageEntry *entry = nullptr;
    if (meta == nullptr) {
      assert(old_meta->Next() == nullptr);
      assert(old_meta->Prev() == nullptr);
      // allocing list is empty, need alloc new page
      meta = global_page_manager->AllocNewPage(slab_class);
      entry = _buffer_pool->FetchNew(meta->PageId(), slab_class);
      LOG_ASSERT(meta != nullptr, "no more page.");

      if (entry == nullptr) {
        entry = _buffer_pool->Evict();
        LOG_ASSERT(entry != nullptr, "evicting failed.");
        // write dirty page back
        if (entry->Dirty) {
          auto batch = _client->BeginBatch();
#ifdef STAT
          stat::dirty_write.fetch_add(1);
#endif
          auto ret = writeToRemote(entry, batch);
          LOG_ASSERT(ret == 0, "rdma write failed.");
          entry->Dirty = false;
          _buffer_pool->InsertPage(entry, meta->PageId(), slab_class);
          _buffer_pool->PinPage(entry);
          meta->Pin();
          _small_allocing_pages[al_index][slab_class] = entry;
          _small_allocing_tail[al_index][slab_class] = meta;
          LOG_ASSERT(_small_allocing_tail[al_index][slab_class] != nullptr, "tail should not be null");
          *batch_ret = batch;
          // ret = batch->FinishBatch();
          // LOG_ASSERT(ret == 0, "write page %d to remote failed.", entry->PageId());
          return entry;
        }
        _buffer_pool->InsertPage(entry, meta->PageId(), slab_class);
      }

      _buffer_pool->PinPage(entry);
      meta->Pin();
      _small_allocing_pages[al_index][slab_class] = entry;
      _small_allocing_tail[al_index][slab_class] = meta;
      LOG_ASSERT(_small_allocing_tail[al_index][slab_class] != nullptr, "tail should not be null");
      return entry;
    }
    // replacement
    LOG_ASSERT(!meta->Full(), "invalid allocing page.");
    global_page_manager->Unmount(old_meta);
    PageId page_id = meta->PageId();
    entry = _buffer_pool->Lookup(page_id, true);
    if (entry == nullptr) {
      entry = _replacement_sgfl.Do(page_id, page_id, _replacement, page_id, slab_class, true);
    }
    _buffer_pool->PinPage(entry);
    meta->Pin();
    _small_allocing_pages[al_index][slab_class] = entry;
    return entry;
  } else {
    PageEntry *old_entry = _big_allocing_pages[slab_class];
    PageMeta *old_meta = global_page_manager->Page(old_entry->PageId());
    _buffer_pool->UnpinPage(old_entry);
    old_meta->UnPin();

    _big_allocing_pages[slab_class] = nullptr;

    PageMeta *meta = old_meta->Next();
    PageEntry *entry = nullptr;
    if (meta == nullptr) {
      assert(old_meta->Next() == nullptr);
      assert(old_meta->Prev() == nullptr);
      // allocing list is empty, need alloc new page
      meta = global_page_manager->AllocNewPage(slab_class);
      entry = _buffer_pool->FetchNew(meta->PageId(), slab_class);
      LOG_ASSERT(meta != nullptr, "no more page.");

      if (entry == nullptr) {
        entry = _buffer_pool->Evict();
        LOG_ASSERT(entry != nullptr, "evicting failed.");
        // write dirty page back
        if (entry->Dirty) {
          auto batch = _client->BeginBatch();
#ifdef STAT
          stat::dirty_write.fetch_add(1);
#endif
          auto ret = writeToRemote(entry, batch);
          LOG_ASSERT(ret == 0, "rdma write failed.");
          entry->Dirty = false;
          _buffer_pool->InsertPage(entry, meta->PageId(), slab_class);
          _buffer_pool->PinPage(entry);
          meta->Pin();
          _big_allocing_pages[slab_class] = entry;
          _big_allocing_tail[slab_class] = meta;
          *batch_ret = batch;
          // ret = batch->FinishBatch();
          // LOG_ASSERT(ret == 0, "write page %d to remote failed.", entry->PageId());
          return entry;
        }
        _buffer_pool->InsertPage(entry, meta->PageId(), slab_class);
      }

      _buffer_pool->PinPage(entry);
      meta->Pin();
      _big_allocing_pages[slab_class] = entry;
      _big_allocing_tail[slab_class] = meta;
      return entry;
    }
    // replacement
    LOG_ASSERT(!meta->Full(), "invalid allocing page.");  // TODO: 这里也assert了
    global_page_manager->Unmount(old_meta);
    PageId page_id = meta->PageId();
    entry = _buffer_pool->Lookup(page_id, true);
    if (entry == nullptr) {
      entry = _replacement_sgfl.Do(page_id, page_id, _replacement, page_id, slab_class, true);
    }
    _buffer_pool->PinPage(entry);
    meta->Pin();
    _big_allocing_pages[slab_class] = entry;
    return entry;
  }
}

// TODO: 目前一个page64KB，可以考虑对数据压缩一下，这样网络传输速度会快一些
PageEntry *Pool::replacement(PageId page_id, uint8_t slab_class, bool writer) {
  // miss
#ifdef STAT
  stat::replacement.fetch_add(1);
  if (stat::replacement.load(std::memory_order_relaxed) % 10000 == 0) {
    LOG_INFO("Replacement %ld", stat::replacement.load(std::memory_order_relaxed));
  }
#endif
  // recheck
  // 这里必须要recheck，因为在调用replacement之前的check中，虽然这个page_id不在本地，但是由于没有锁的保证，所以可能在你check完之后它又在了
  // 如果不recheck，那么会从远端读取数据，而本地的那个可能发生更改，那么就会导致远端的旧数据覆盖本地写入的新数据，导致读取发生错误
  PageEntry *entry = _buffer_pool->Lookup(page_id, writer);
  if (entry != nullptr) {
    return entry;
  }

  PageEntry *victim = _buffer_pool->Evict();
  auto batch = _client->BeginBatch();
  if (victim->Dirty) {
#ifdef STAT
    stat::dirty_write.fetch_add(1);
#endif
    auto ret = writeToRemote(victim, batch);
    LOG_ASSERT(ret == 0, "write page %d to remote failed.", victim->PageId());
    victim->Dirty = false;
  }
  auto ret = readFromRemote(victim, page_id, batch);
  ret = batch->FinishBatch();
  delete batch;
  _buffer_pool->InsertPage(victim, page_id, slab_class);
  LOG_ASSERT(ret == 0, "write page %d to remote failed.", victim->PageId());
  return victim;
}

bool Pool::writeNew(const Slice &key, uint32_t hash, const Slice &val) {
#ifdef STAT
  stat::insert_num.fetch_add(1);
#endif
  // write key
  KeySlot *slot = _hash_index->Insert(key, hash);
  slot->SetKey(key);

  // write value
  uint8_t slab_class = val.size() / kSlabSize;

  int al_index = (hash >> 24) % kAllocingListShard;
  PageEntry *page;
  int off;
  PageMeta *meta;
  RDMAManager::Batch *batch = nullptr;
  // if (!isSmallSlabSize(slab_class)) {
  //   _big_allocing_list_latch[slab_class].WLock();
  // }
  allocingListWLock(al_index, slab_class);
  if (LIKELY(isSmallSlabSize(slab_class))) {
    page = _small_allocing_pages[al_index][slab_class];
    meta = global_page_manager->Page(page->PageId());
    if (UNLIKELY(meta->Full())) {
      page = mountNewPage(slab_class, hash, &batch, -1);
      meta = global_page_manager->Page(page->PageId());
    }
    // modify bitmap
    off = meta->SetFirstFreePos();
  } else {
    page = _big_allocing_pages[slab_class];
    meta = global_page_manager->Page(page->PageId());
    if (UNLIKELY(meta->Full())) {
      page = mountNewPage(slab_class, hash, &batch, -1);
      meta = global_page_manager->Page(page->PageId());
    }
    // modify bitmap
    off = meta->SetFirstFreePos();
  }
  if (batch != nullptr) {
    batch->FinishBatch();
    delete batch;
  }
  // if (!isSmallSlabSize(slab_class)) {
  //   _big_allocing_list_latch[slab_class].WUnlock();
  // }
  allocingListWUnlock(al_index, slab_class);

  LOG_ASSERT(off != -1, "set bitmap failed.");
  Addr addr = AddrParser::GenAddrFrom(meta->PageId(), off);
  slot->SetAddr(addr);

  // copy data
  my_memcpy((void *)(page->Data() + off * val.size()), val.data(), val.size());
  if (!page->Dirty) page->Dirty = true;
  _buffer_pool->Release(page);

  return true;
}

int Pool::writeToRemote(PageEntry *entry, RDMAManager::Batch *batch) {
  uint32_t block = AddrParser::GetBlockFromPageId(entry->PageId());
  uint32_t block_off = AddrParser::GetBlockOffFromPageId(entry->PageId());
  LOG_DEBUG("write to block %d off %d", block, block_off);
  const MemoryAccess &access = _access_table->at(block);
  return batch->RemoteWrite(entry->Data(), _buffer_pool->MR()->lkey, kPageSize, access.addr + kPageSize * block_off,
                            access.rkey);
}

int Pool::readFromRemote(PageEntry *entry, PageId page_id, RDMAManager::Batch *batch) {
  uint32_t block = AddrParser::GetBlockFromPageId(page_id);
  uint32_t block_off = AddrParser::GetBlockOffFromPageId(page_id);
  const MemoryAccess &access = _access_table->at(block);
  return batch->RemoteRead(entry->Data(), _buffer_pool->MR()->lkey, kPageSize, access.addr + kPageSize * block_off,
                           access.rkey);
}

}  // namespace kv