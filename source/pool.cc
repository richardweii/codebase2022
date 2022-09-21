#include "pool.h"
#include "stat.h"
#include "util/likely.h"

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

  for (int i = kSlabSizeMin; i <= kSlabSizeMax; i++) {
    PageMeta *page = global_page_manager->AllocNewPage(i);
    _allocing_tail[i] = page;
    _allocing_pages[i] = _buffer_pool->FetchNew(page->PageId(), i);
    _buffer_pool->PinPage(_allocing_pages[i]);
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
    memcpy((char *)val.data(), entry->Data() + val_len * AddrParser::Off(addr), val_len);
    _buffer_pool->Release(entry);
    return true;
  }

  // cache miss
  PageEntry *victim = _replacement_sgfl.Do(page_id, page_id, _replacement, page_id, meta->SlabClass(), false);
  uint32_t val_len = victim->SlabClass() * kSlabSize;
  val.resize(val_len);
  memcpy((char *)val.data(), victim->Data() + val_len * AddrParser::Off(addr), val_len);

  _buffer_pool->Release(victim);
  return true;
}

bool Pool::Write(const Slice &key, uint32_t hash, const Slice &val) {
  KeySlot *slot = _hash_index->Find(key, hash);
  if (slot == nullptr) {
    // insert new KV
    writeNew(key, hash, val);
    // _write_new_sgfl.Do(key.toString(), hash, _writeNew, key, hash, val);
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

      memcpy((char *)(entry->Data() + val.size() * AddrParser::Off(addr)), val.data(), val.size());
      _buffer_pool->Release(entry);
      return true;
    }

    // cache miss
    // _lock.Lock();
    // defer { _lock.Unlock(); };
    PageEntry *victim = _replacement_sgfl.Do(page_id, page_id, _replacement, page_id, meta->SlabClass(), true);
    memcpy((char *)(victim->Data() + val.size() * AddrParser::Off(addr)), val.data(), val.size());
    if (!victim->Dirty) victim->Dirty = true;

    _buffer_pool->Release(victim);
    return true;
  }

  modifyLength(slot, val);
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

  _allocing_list_latch[meta->SlabClass()].WLock();
  meta->ClearPos(AddrParser::Off(addr));
  global_page_manager->Mount(&_allocing_tail[meta->SlabClass()], meta);
  if (meta->Empty() && _allocing_pages[meta->SlabClass()]->PageId() != page_id) {
    if (_allocing_tail[meta->SlabClass()]->PageId() == page_id) {
      _allocing_tail[meta->SlabClass()] = meta->Prev();
      // LOG_DEBUG("[shard %d] set class %d tail page %d", _shard, meta->SlabClass(), meta->Prev()->PageId());
    }
    global_page_manager->Unmount(meta);
    global_page_manager->FreePage(page_id);
  }
  _allocing_list_latch[meta->SlabClass()].WUnlock();

  _hash_index->GetSlotMonitor()->FreeSlot(slot);
  return true;
}

void Pool::modifyLength(KeySlot *slot, const Slice &val) {
  // delete
  Addr addr = slot->Addr();
  PageId page_id = AddrParser::PageId(addr);
  PageMeta *meta = global_page_manager->Page(page_id);
  assert(meta->SlabClass() != val.size() / kSlabSize);

  _allocing_list_latch[meta->SlabClass()].WLock();
  meta->ClearPos(AddrParser::Off(addr));
  global_page_manager->Mount(&_allocing_tail[meta->SlabClass()], meta);
  if (meta->Empty() && _allocing_pages[meta->SlabClass()]->PageId() != page_id) {
    if (_allocing_tail[meta->SlabClass()]->PageId() == page_id) {
      _allocing_tail[meta->SlabClass()] = meta->Prev();
      // LOG_DEBUG("[shard %d] set class %d tail page %d", _shard, meta->SlabClass(), meta->Prev()->PageId());
    }
    global_page_manager->Unmount(meta);
    global_page_manager->FreePage(page_id);
  }
  _allocing_list_latch[meta->SlabClass()].WUnlock();

  // rewrite to meta
  uint8_t slab_class = val.size() / kSlabSize;

  _allocing_list_latch[slab_class].WLock();
  PageEntry *page = _allocing_pages[slab_class];
  meta = global_page_manager->Page(page->PageId());
  if (meta->Full()) {
    page = mountNewPage(slab_class);
    meta = global_page_manager->Page(page->PageId());
  } else {
    page->WLock();
  }
  // modify bitmap
  int off = meta->SetFirstFreePos();
  _allocing_list_latch[slab_class].WUnlock();

  memcpy((char *)(page->Data() + val.size() * off), val.data(), val.size());
  if (!page->Dirty) page->Dirty = true;
  _buffer_pool->Release(page);

  LOG_ASSERT(off != -1, "set bitmap failed.");
  addr = AddrParser::GenAddrFrom(meta->PageId(), off);
  slot->SetAddr(addr);
}

PageEntry *Pool::mountNewPage(uint8_t slab_class) {
  PageEntry *old_entry = _allocing_pages[slab_class];
  PageMeta *old_meta = global_page_manager->Page(old_entry->PageId());
  _buffer_pool->UnpinPage(old_entry);

  _allocing_pages[slab_class] = nullptr;

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
        auto ret = writeToRemote(entry, &batch);
        LOG_ASSERT(ret == 0, "rdma write failed.");
        entry->Dirty = false;
        ret = batch.FinishBatch();
        LOG_ASSERT(ret == 0, "write page %d to remote failed.", entry->PageId());
      }
      _buffer_pool->InsertPage(entry, meta->PageId(), slab_class);
    } else {
      entry->WLock();
    }

    _buffer_pool->PinPage(entry);
    _allocing_pages[slab_class] = entry;
    _allocing_tail[slab_class] = meta;
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
  _allocing_pages[slab_class] = entry;
  return entry;
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
    auto ret = writeToRemote(victim, &batch);
    LOG_ASSERT(ret == 0, "write page %d to remote failed.", victim->PageId());
    victim->Dirty = false;
  }
  auto ret = readFromRemote(victim, page_id, &batch);
  ret = batch.FinishBatch();
  LOG_ASSERT(ret == 0, "write page %d to remote failed.", victim->PageId());

  _buffer_pool->InsertPage(victim, page_id, slab_class);
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

  _allocing_list_latch[slab_class].WLock();
  PageEntry *page = _allocing_pages[slab_class];
  PageMeta *meta = global_page_manager->Page(page->PageId());
  // TODO：火焰图上这里的Full占用太多
  if (UNLIKELY(meta->Full())) {
    page = mountNewPage(slab_class);
    meta = global_page_manager->Page(page->PageId());
  } else {
    page->WLock();
  }
  // modify bitmap
  int off = meta->SetFirstFreePos();
  _allocing_list_latch[slab_class].WUnlock();

  // LOG_ASSERT(off != -1, "set bitmap failed.");
  Addr addr = AddrParser::GenAddrFrom(meta->PageId(), off);
  slot->SetAddr(addr);

  // copy data
  memcpy((void *)(page->Data() + off * val.size()), val.data(), val.size());
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