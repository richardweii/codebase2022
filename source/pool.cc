#include "pool.h"

namespace kv {

Pool::Pool(uint8_t shard, RDMAClient *client) : _client(client), _shard(shard) {
  _buffer_pool = new BufferPool(kBufferPoolSize / kPoolShardingNum);
  _hash_index = new HashTable(kKeyNum / kPoolShardingNum, _slots);
  _page_manager = new PageManager(kPoolShardingSize / kPageSize);
}

Pool::~Pool() {
  // _hash_index->PrintCounter();
  delete _buffer_pool;
  delete _hash_index;
  delete _page_manager;
}

void Pool::Init() {
  auto succ = _buffer_pool->Init(_client->Pd());
  LOG_ASSERT(succ, "Init buffer pool failed.");
  AllocRequest req;
  req.shard = _shard;
  req.size = kPoolShardingSize;
  req.type = MSG_ALLOC;

  AllocResponse resp;
  _client->RPC(req, resp);
  if (resp.status != RES_OK) {
    LOG_FATAL("Failed to alloc new block.");
    return;
  }

  _rdma_access.addr = resp.addr;
  _rdma_access.rkey = resp.rkey;

  for (int i = kSlabSizeMin; i <= kSlabSizeMax; i++) {
    PageMeta *page = _page_manager->AllocNewPage(i);
    _allocing_pages[i] = _buffer_pool->FetchNew(page->PageId());
    _allocing_pages[i]->SlabClass = i;
    _buffer_pool->PinPage(_allocing_pages[i]);
  }

  _free_slot_head = 0;
  for (size_t i = 0; i < (kKeyNum / kPoolShardingNum) - 1; i++) {
    _slots[i].SetNext(i + 1);
  }
}

bool Pool::Read(const Slice &key, uint32_t hash, std::string &val) {
  // existence
  KeySlot *slot = _hash_index->Find(key, hash);
  if (slot == nullptr) {
#ifdef STAT
    stat::read_miss.fetch_add(1);
#endif
    return false;
  }
  Addr addr = slot->Addr();
  PageMeta *meta = _page_manager->Page(AddrParser::PageId(addr));
  {  // lock-free phase
    _latch.RLock();
    defer { _latch.RUnlock(); };
    // cache
    PageEntry *entry = _buffer_pool->Lookup(AddrParser::PageId(addr));

    if (entry != nullptr) {
#ifdef STAT
      stat::cache_hit.fetch_add(1, std::memory_order_relaxed);
#endif
      uint32_t val_len = entry->SlabClass * kSlabSize;
      val.resize(val_len);
      memcpy((char *)val.data(), entry->Data() + val_len * AddrParser::Off(addr), val_len);
      _buffer_pool->Release(entry);
      return true;
    }
  }
  {
    _latch.WLock();
    defer { _latch.WUnlock(); };

    // recheck
    PageEntry *entry = _buffer_pool->Lookup(AddrParser::PageId(addr));
    if (entry != nullptr) {
#ifdef STAT
      stat::cache_hit.fetch_add(1, std::memory_order_relaxed);
#endif
      uint32_t val_len = entry->SlabClass * kSlabSize;
      val.resize(val_len);
      memcpy((char *)val.data(), entry->Data() + val_len * AddrParser::Off(addr), val_len);
      _buffer_pool->Release(entry);
      return true;
    }

    // cache miss
    PageEntry *victim = replacement(addr, meta->SlabClass());

    uint32_t val_len = victim->SlabClass * kSlabSize;
    val.resize(val_len);
    memcpy((char *)val.data(), victim->Data() + val_len * AddrParser::Off(addr), val_len);

    _buffer_pool->Release(victim);
    return true;
  }
}

bool Pool::Write(const Slice &key, uint32_t hash, const Slice &val) {
  KeySlot *slot = _hash_index->Find(key, hash);
  Addr addr = INVALID_ADDR;
  PageMeta *meta = nullptr;
  {
    _latch.RLock();
    defer { _latch.RUnlock(); };
    // lock-free phase
    if (slot != nullptr) {
      addr = slot->Addr();
      meta = _page_manager->Page(AddrParser::PageId(addr));
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
    defer { _latch.WUnlock(); };
    if (slot == nullptr) {
      // insert new KV
      writeNew(key, hash, val);
#ifdef STAT
      if (stat::insert_num == 160000001) {
        LOG_INFO("Finish insert.");
      }
#endif
    } else {
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

        // cache miss
        PageEntry *victim = replacement(addr, meta->SlabClass());

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
  PageMeta *meta = _page_manager->Page(page_id);
  meta->ClearPos(AddrParser::Off(addr));
  if (meta->Empty() && _allocing_pages[meta->SlabClass()]->PageId() != page_id) {
    _page_manager->FreePage(page_id);
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
  PageMeta *meta = _page_manager->Page(page_id);
  assert(meta->SlabClass() != val.size() / kSlabSize);
  meta->ClearPos(AddrParser::Off(addr));
  if (meta->Empty() && _allocing_pages[meta->SlabClass()]->PageId() != page_id) {
    _page_manager->FreePage(page_id);
  }
  // rewrite to meta
  uint8_t slab_class = val.size() / kSlabSize;

  PageEntry *page = _allocing_pages[slab_class];
  meta = _page_manager->Page(page->PageId());
  if (meta->Full()) {
    unmountPage(slab_class);
    page = mountNewPage(slab_class);
    meta = _page_manager->Page(page->PageId());
  }

  // modify bitmap
  int off = meta->SetFirstFreePos();
  memcpy((char *)(page->Data() + val.size() * off), val.data(), val.size());
  if (!page->Dirty) page->Dirty = true;

  LOG_ASSERT(off != -1, "set bitmap failed.");
  addr = AddrParser::GenAddrFrom(meta->PageId(), off);
  slot->SetAddr(addr);
}

void Pool::unmountPage(uint8_t slab_class) {
  PageEntry *entry = _allocing_pages[slab_class];
  _buffer_pool->UnpinPage(entry);
  _buffer_pool->Release(entry);
  _allocing_pages[slab_class] = nullptr;
}

PageEntry *Pool::mountNewPage(uint8_t slab_class) {
  PageMeta *meta = _page_manager->AllocNewPage(slab_class);
  LOG_ASSERT(meta != nullptr, "no more page.");

  PageEntry *entry = _buffer_pool->FetchNew(meta->PageId());
  if (entry == nullptr) {
    entry = _buffer_pool->Evict();
    LOG_ASSERT(entry != nullptr, "evicting failed.");
    // write dirty page back
    auto batch = _client->BeginBatch();
    if (entry->Dirty) {
      auto ret = writeToRemote(entry, &batch);
      LOG_ASSERT(ret == 0, "rdma write failed.");
      entry->Dirty = false;
    }
    auto ret = batch.FinishBatch();
    LOG_ASSERT(ret == 0, "write page %d to remote failed.", entry->PageId());
    _buffer_pool->InsertPage(entry, meta->PageId());
  }

  entry->SlabClass = slab_class;
  _buffer_pool->PinPage(entry);
  _allocing_pages[slab_class] = entry;
  return entry;
}

PageEntry *Pool::replacement(Addr addr, uint8_t slab_class) {
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
  auto ret = readFromRemote(victim, addr, &batch);
  ret = batch.FinishBatch();
  LOG_ASSERT(ret == 0, "write page %d to remote failed.", victim->PageId());

  _buffer_pool->InsertPage(victim, AddrParser::PageId(addr));

  victim->SlabClass = slab_class;
  return victim;
}

void Pool::writeNew(const Slice &key, uint32_t hash, const Slice &val) {
#ifdef STAT
  stat::insert_num.fetch_add(1);
#endif
  // write key
  KeySlot *slot = &_slots[_free_slot_head];
  _hash_index->Insert(key, hash, _free_slot_head);
  _free_slot_head = slot->Next();
  slot->SetKey(key);
  slot->SetNext(-1);

  // write value
  uint8_t slab_class = val.size() / kSlabSize;

  PageEntry *page = _allocing_pages[slab_class];
  PageMeta *meta = _page_manager->Page(page->PageId());
  if (meta->Full()) {
    unmountPage(slab_class);
    page = mountNewPage(slab_class);
    meta = _page_manager->Page(page->PageId());
  }

  // modify bitmap
  int off = meta->SetFirstFreePos();
  // LOG_ASSERT(off != -1, "set bitmap failed.");
  Addr addr = AddrParser::GenAddrFrom(meta->PageId(), off);
  slot->SetAddr(addr);

  // copy data
  memcpy((void *)(page->Data() + off * val.size()), val.data(), val.size());
  if (!page->Dirty) page->Dirty = true;
}

int Pool::writeToRemote(PageEntry *entry, RDMAManager::Batch *batch) {
  return batch->RemoteWrite(entry->Data(), _buffer_pool->MR()->lkey, kPageSize,
                            _rdma_access.addr + kPageSize * entry->PageId(), _rdma_access.rkey);
}

int Pool::readFromRemote(PageEntry *entry, Addr addr, RDMAManager::Batch *batch) {
  return batch->RemoteRead(entry->Data(), _buffer_pool->MR()->lkey, kPageSize,
                           _rdma_access.addr + kPageSize * AddrParser::PageId(addr), _rdma_access.rkey);
}

}  // namespace kv