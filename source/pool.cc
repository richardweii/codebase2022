#include "pool.h"
#include "stat.h"

namespace kv {

Pool::Pool(uint8_t shard, RDMAClient *client, std::vector<MemoryAccess> *global_rdma_access)
    : _access_table(global_rdma_access), _client(client), _shard(shard) {
  _buffer_pool = new BufferPool(kBufferPoolSize / kPoolShardingNum, shard);
  _hash_index = new HashTable(kKeyNum / kPoolShardingNum, _slots);
}

Pool::~Pool() {
  // _hash_index->PrintCounter();
  delete _buffer_pool;
  delete _hash_index;
}

void Pool::Init() {
  auto succ = _buffer_pool->Init(_client->Pd());
  LOG_ASSERT(succ, "Init buffer pool failed.");

  for (int i = kSlabSizeMin; i <= kSlabSizeMax; i++) {
    PageMeta *page = global_page_manger->AllocNewPage(i);
    _allocing_tail[i] = page;
    _allocing_pages[i] = _buffer_pool->FetchNew(page->PageId(), i);
    _buffer_pool->PinPage(_allocing_pages[i]);
  }

  _free_slot_head = 0;
  for (size_t i = 0; i < (kKeyNum / kPoolShardingNum) - 1; i++) {
    _slots[i].SetNext(i + 1);
  }
}

bool Pool::Read(const Slice &key, uint32_t hash, std::string &val) {
  if (!bind_core.isDone()) {
    bind_core.bind();
  }
  // existence
  KeySlot *slot = _hash_index->Find(key, hash);
  if (slot == nullptr) {
#ifdef STAT
    stat::read_miss.fetch_add(1);
#endif
    return false;
  }
  Addr addr = slot->Addr();
  PageId page_id = AddrParser::PageId(addr);
  PageMeta *meta = global_page_manger->Page(page_id);
  {  // lock-free phase
    // _latch.RLock();
    // defer { _latch.RUnlock(); };
    // cache
    PageEntry *entry = _buffer_pool->Lookup(page_id);

    if (entry != nullptr) {
#ifdef STAT
      stat::cache_hit.fetch_add(1, std::memory_order_relaxed);
#endif
      uint32_t val_len = entry->SlabClass() * kSlabSize;
      val.resize(val_len);
      memcpy((char *)val.data(), entry->Data() + val_len * AddrParser::Off(addr), val_len);
      _buffer_pool->Release(entry);
      return true;
    }
  }
  {
    _latch.WLock();

    // recheck
    PageEntry *entry = _buffer_pool->Lookup(page_id);
    if (entry != nullptr) {
#ifdef STAT
      stat::cache_hit.fetch_add(1, std::memory_order_relaxed);
#endif
      _latch.WUnlock();
      uint32_t val_len = entry->SlabClass() * kSlabSize;
      val.resize(val_len);
      memcpy((char *)val.data(), entry->Data() + val_len * AddrParser::Off(addr), val_len);
      _buffer_pool->Release(entry);
      return true;
    }

    // cache miss
    PageEntry *victim = replacement(page_id, meta->SlabClass());
    _latch.WUnlock();

    uint32_t val_len = victim->SlabClass() * kSlabSize;
    val.resize(val_len);
    memcpy((char *)val.data(), victim->Data() + val_len * AddrParser::Off(addr), val_len);

    _buffer_pool->Release(victim);
    return true;
  }
}

bool Pool::Write(const Slice &key, uint32_t hash, const Slice &val) {
  if (!bind_core.isDone()) {
    bind_core.bind();
  }
  KeySlot *slot = nullptr;
  PageMeta *meta = nullptr;
  Addr addr = INVALID_ADDR;
  {
    // _latch.RLock();
    // defer { _latch.RUnlock(); };
    slot = _hash_index->Find(key, hash);
    // lock-free phase
    if (slot != nullptr) {
      addr = slot->Addr();
      meta = global_page_manger->Page(AddrParser::PageId(addr));
      // modify in place
      if (meta->SlabClass() == val.size() / kSlabSize) {
        PageEntry *entry = _buffer_pool->Lookup(AddrParser::PageId(addr));
        if (entry != nullptr) {
#ifdef STAT
          stat::cache_hit.fetch_add(1, std::memory_order_relaxed);
#endif
          if (!entry->Dirty) entry->Dirty = true;

          memcpy((char *)(entry->Data() + val.size() * AddrParser::Off(addr)), val.data(), val.size());
          _buffer_pool->Release(entry);
          return true;
        }
      }
    }
  }
  {
    _latch.WLock();
    if (slot == nullptr) {
      // insert new KV
      writeNew(key, hash, val);
    } else {
      if (meta->SlabClass() == val.size() / kSlabSize) {
        PageId page_id = AddrParser::PageId(addr);
        PageEntry *entry = _buffer_pool->Lookup(page_id);
        if (entry != nullptr) {
#ifdef STAT
          stat::cache_hit.fetch_add(1, std::memory_order_relaxed);
#endif
          _latch.WUnlock();
          if (!entry->Dirty) entry->Dirty = true;

          memcpy((char *)(entry->Data() + val.size() * AddrParser::Off(addr)), val.data(), val.size());
          _buffer_pool->Release(entry);
          return true;
        }

        // cache miss
        PageEntry *victim = replacement(page_id, meta->SlabClass());
        _latch.WUnlock();

        memcpy((char *)(victim->Data() + val.size() * AddrParser::Off(addr)), val.data(), val.size());
        if (!victim->Dirty) victim->Dirty = true;

        _buffer_pool->Release(victim);
        return true;
      }
      modifyLength(slot, val);
    }
  }
  return true;
}

bool Pool::Delete(const Slice &key, uint32_t hash) {
  if (!bind_core.isDone()) {
    bind_core.bind();
  }
  _latch.WLock();
  defer { _latch.WUnlock(); };
  int slot_idx = _hash_index->Remove(key, hash);
  if (slot_idx == KeySlot::INVALID_SLOT_ID) {
    LOG_ERROR("delete invalid file %s", key.data());
    return false;
  }
  KeySlot *slot = &_slots[slot_idx];
  Addr addr = slot->Addr();
  PageId page_id = AddrParser::PageId(addr);
  PageMeta *meta = global_page_manger->Page(page_id);
  meta->ClearPos(AddrParser::Off(addr));
  global_page_manger->Mount(&_allocing_tail[meta->SlabClass()], meta);
  if (meta->Empty() && _allocing_pages[meta->SlabClass()]->PageId() != page_id) {
    if (_allocing_tail[meta->SlabClass()]->PageId() == page_id) {
      _allocing_tail[meta->SlabClass()] = meta->Prev();
      // LOG_DEBUG("[shard %d] set class %d tail page %d", _shard, meta->SlabClass(), meta->Prev()->PageId());
    }
    global_page_manger->Unmount(meta);
    global_page_manger->FreePage(page_id);
  }

  slot->SetAddr(INVALID_ADDR);
  slot->SetNext(_free_slot_head);
  _free_slot_head = slot_idx;
  return true;
}

void Pool::modifyLength(KeySlot *slot, const Slice &val) {
  // delete
  Addr addr = slot->Addr();
  PageId page_id = AddrParser::PageId(addr);
  PageMeta *meta = global_page_manger->Page(page_id);
  assert(meta->SlabClass() != val.size() / kSlabSize);
  meta->ClearPos(AddrParser::Off(addr));
  global_page_manger->Mount(&_allocing_tail[meta->SlabClass()], meta);
  if (meta->Empty() && _allocing_pages[meta->SlabClass()]->PageId() != page_id) {
    if (_allocing_tail[meta->SlabClass()]->PageId() == page_id) {
      _allocing_tail[meta->SlabClass()] = meta->Prev();
      // LOG_DEBUG("[shard %d] set class %d tail page %d", _shard, meta->SlabClass(), meta->Prev()->PageId());
    }
    global_page_manger->Unmount(meta);
    global_page_manger->FreePage(page_id);
  }
  // rewrite to meta
  uint8_t slab_class = val.size() / kSlabSize;

  PageEntry *page = _allocing_pages[slab_class];
  meta = global_page_manger->Page(page->PageId());
  if (meta->Full()) {
    page = mountNewPage(slab_class);
    meta = global_page_manger->Page(page->PageId());
  }

  // modify bitmap
  int off = meta->SetFirstFreePos();
  _latch.WUnlock();

  memcpy((char *)(page->Data() + val.size() * off), val.data(), val.size());
  if (!page->Dirty) page->Dirty = true;

  LOG_ASSERT(off != -1, "set bitmap failed.");
  addr = AddrParser::GenAddrFrom(meta->PageId(), off);
  slot->SetAddr(addr);
}

PageEntry *Pool::mountNewPage(uint8_t slab_class) {
  PageEntry *old_entry = _allocing_pages[slab_class];
  PageMeta *old_meta = global_page_manger->Page(old_entry->PageId());
  _buffer_pool->UnpinPage(old_entry);
  _buffer_pool->Release(old_entry);
  _allocing_pages[slab_class] = nullptr;

  PageMeta *meta = old_meta->Next();
  PageEntry *entry = nullptr;
  if (meta == nullptr) {
    // allocing list is empty, need alloc new page
    meta = global_page_manger->AllocNewPage(slab_class);
    entry = _buffer_pool->FetchNew(meta->PageId(), slab_class);
    LOG_ASSERT(meta != nullptr, "no more page.");

    if (entry == nullptr) {
      entry = _buffer_pool->Evict();
      LOG_ASSERT(entry != nullptr, "evicting failed.");
      // write dirty page back
      auto batch = _client->BeginBatch();
      if (entry->Dirty) {
#ifdef STAT
        stat::dirty_write.fetch_add(1);
#endif
        auto ret = writeToRemote(entry, &batch);
        LOG_ASSERT(ret == 0, "rdma write failed.");
        entry->Dirty = false;
      }
      auto ret = batch.FinishBatch();
      LOG_ASSERT(ret == 0, "write page %d to remote failed.", entry->PageId());
      _buffer_pool->InsertPage(entry, meta->PageId(), slab_class);
    }

    _buffer_pool->PinPage(entry);
    _allocing_pages[slab_class] = entry;
    _allocing_tail[slab_class] = meta;
    return entry;
  }
  // replacement
  LOG_ASSERT(!meta->Full(), "invalid allocing page.");
  global_page_manger->Unmount(old_meta);
  entry = _buffer_pool->Lookup(meta->PageId());
  if (entry == nullptr) {
    entry = replacement(meta->PageId(), slab_class);
  }
  _buffer_pool->PinPage(entry);
  _allocing_pages[slab_class] = entry;
  return entry;
}

// TODO: 目前一个page64KB，可以考虑对数据压缩一下，这样网络传输速度会快一些
PageEntry *Pool::replacement(PageId page_id, uint8_t slab_class) {
  // miss
#ifdef STAT
  stat::replacement.fetch_add(1);
  if (stat::replacement.load(std::memory_order_relaxed) % 10000 == 0) {
    LOG_INFO("Replacement %ld", stat::replacement.load(std::memory_order_relaxed));
  }
#endif
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

void Pool::writeNew(const Slice &key, uint32_t hash, const Slice &val) {
#ifdef STAT
  stat::insert_num.fetch_add(1);
#endif
  // write key
  int insert_slot = _free_slot_head;
  KeySlot *slot = &_slots[_free_slot_head];
  _free_slot_head = slot->Next();
  slot->SetKey(key);
  slot->SetNext(-1);

  _hash_index->Insert(key, hash, insert_slot);

  // write value
  uint8_t slab_class = val.size() / kSlabSize;

  PageEntry *page = _allocing_pages[slab_class];
  PageMeta *meta = global_page_manger->Page(page->PageId());
  // TODO：火焰图上这里的Full占用太多
  if (meta->Full()) {
    page = mountNewPage(slab_class);
    meta = global_page_manger->Page(page->PageId());
  }

  // modify bitmap
  int off = meta->SetFirstFreePos();
  _latch.WUnlock();
  // LOG_ASSERT(off != -1, "set bitmap failed.");
  Addr addr = AddrParser::GenAddrFrom(meta->PageId(), off);
  slot->SetAddr(addr);

  // copy data
  memcpy((void *)(page->Data() + off * val.size()), val.data(), val.size());
  if (!page->Dirty) page->Dirty = true;
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