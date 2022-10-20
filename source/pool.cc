#include "pool.h"
#include <infiniband/verbs.h>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <vector>
#include "buffer_pool.h"
#include "config.h"
#include "msg.h"
#include "page_manager.h"
#include "stat.h"
#include "util/likely.h"
#include "util/logging.h"
#include "util/lz4.h"
#include "util/memcpy.h"
#include "util/rwlock.h"

namespace kv {
HashTable *table = new HashTable(kHashIndexSize);
BufferPool *buffer_pool = new BufferPool(kBufferPoolSize / kPoolShardingNum);
Pool::Pool(RDMAClient *client, MemoryAccess *global_rdma_access)
    : _access_table(global_rdma_access), _client(client) {
  _buffer_pool = buffer_pool;
  _hash_index = table;
  _replacement =
      std::bind(&Pool::replacement, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
}

Pool::~Pool() {
  delete _buffer_pool;
  delete _hash_index;
}

void Pool::Init() {
  auto succ = _buffer_pool->Init(_client->Pd());
  LOG_ASSERT(succ, "Init buffer pool failed.");

  for (int j = 0; j < kAllocingListShard; j++) {
    for (int i = kSlabSizeMin; i <= kSlabSizeMax; i++) {
      PageMeta *page = global_page_manager->AllocNewPage(i, j);
      _allocing_tail[j][i] = page;
      _allocing_pages[j][i] = _buffer_pool->FetchNew(page->PageId(), i, j);
      _buffer_pool->PinPage(_allocing_pages[j][i]);
      _allocing_tail[j][i]->Pin();
      _allocing_tail[j][i]->al_index = j;
    }
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
    uint32_t val_len = meta->SlabClass() * kSlabSize;
    val.resize(val_len);
    my_memcpy((char *)val.data(), entry->Data() + val_len * AddrParser::Off(addr), val_len);
    _buffer_pool->Release(entry);
    return true;
  }

  // cache miss
  PageEntry *victim = _replacement_sgfl.Do(page_id, page_id, _replacement, page_id, meta->SlabClass(), false);
  PageMeta *victim_meta = global_page_manager->Page(victim->PageId());
  uint32_t val_len = victim_meta->SlabClass() * kSlabSize;
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
    // stat::replacement++;
    PageEntry *victim = replacement(page_id, meta->SlabClass(), true);
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

  int al_index = meta->al_index;
  LOG_ASSERT(al_index >= 0 && al_index <= 15, "bound error");
  allocingListWLock(al_index, meta->SlabClass());
  meta->ClearPos(AddrParser::Off(addr));
  global_page_manager->Mount(&_allocing_tail[al_index][meta->SlabClass()], meta);
  if (!meta->IsPined() && meta->Empty()) {
    if (_allocing_tail[al_index][meta->SlabClass()]->PageId() == page_id) {
      _allocing_tail[al_index][meta->SlabClass()] = meta->Prev();
      LOG_ASSERT(_allocing_tail[al_index][meta->SlabClass()] != nullptr, "tail should not be null");
    }
    global_page_manager->Unmount(meta);
    global_page_manager->FreePage(page_id);
  }
  allocingListWUnlock(al_index, meta->SlabClass());

  _hash_index->GetSlotMonitor()->FreeSlot(slot);
  return true;
}

void Pool::modifyLength(KeySlot *slot, const Slice &val, uint32_t hash) {
  // delete
  Addr addr = slot->Addr();
  PageId page_id = AddrParser::PageId(addr);
  PageMeta *meta = global_page_manager->Page(page_id);
  assert(meta->SlabClass() != val.size() / kSlabSize);

  int al_index = meta->al_index;
  LOG_ASSERT(al_index >= 0 && al_index <= 15, "bound error");
  allocingListWLock(al_index, meta->SlabClass());
  meta->ClearPos(AddrParser::Off(addr));
  global_page_manager->Mount(&_allocing_tail[al_index][meta->SlabClass()], meta);
  if (!meta->IsPined() && meta->Empty()) {
    if (_allocing_tail[al_index][meta->SlabClass()]->PageId() == page_id) {
      _allocing_tail[al_index][meta->SlabClass()] = meta->Prev();
      LOG_ASSERT(_allocing_tail[al_index][meta->SlabClass()] != nullptr, "tail should not be null");
    }
    global_page_manager->Unmount(meta);
    global_page_manager->FreePage(page_id);
  }
  allocingListWUnlock(al_index, meta->SlabClass());

  // rewrite to meta
  uint8_t slab_class = val.size() / kSlabSize;

  PageEntry *page;
  RDMAManager::Batch *batch = nullptr;
  int off;
  LOG_ASSERT(al_index >= 0 && al_index <= 15, "bound error");
  page = _allocing_pages[al_index][slab_class];
  meta = global_page_manager->Page(page->PageId());
  if (UNLIKELY(meta->Full())) {
    page = mountNewPage(slab_class, hash, &batch, al_index);
    meta = global_page_manager->Page(page->PageId());
  }
  LOG_ASSERT(!meta->Full(), "meta should not full");
  // modify bitmap
  off = meta->SetFirstFreePos();
  LOG_ASSERT(off != -1, "set bitmap failed.");
  if (batch != nullptr) {
    batch->FinishBatchTL();
  }

  my_memcpy((char *)(page->Data() + val.size() * off), val.data(), val.size());
  if (!page->Dirty) page->Dirty = true;
  _buffer_pool->Release(page);

  LOG_ASSERT(off != -1, "set bitmap failed.");
  addr = AddrParser::GenAddrFrom(meta->PageId(), off);
  slot->SetAddr(addr);
}

PageEntry *Pool::mountNewPage(uint8_t slab_class, uint32_t hash, RDMAManager::Batch **batch_ret, int tid) {
  int al_index = tid;

  PageEntry *old_entry;
  PageMeta *old_meta;

  old_entry = _allocing_pages[al_index][slab_class];
  _allocing_pages[al_index][slab_class] = nullptr;
  old_meta = global_page_manager->Page(old_entry->PageId());

  PageMeta *meta = old_meta->Next();
  PageEntry *entry = nullptr;
  if (meta == nullptr) {
    assert(old_meta->Next() == nullptr);
    assert(old_meta->Prev() == nullptr);
    // allocing list is empty, need alloc new page
    meta = global_page_manager->AllocNewPage(slab_class, cur_thread_id);
    meta->Pin();
    meta->al_index = al_index;
    entry = _buffer_pool->FetchNew(meta->PageId(), slab_class, cur_thread_id);
    LOG_ASSERT(meta != nullptr, "no more page.");

    if (entry == nullptr) {
      entry = _buffer_pool->Evict();
      LOG_ASSERT(entry != nullptr, "evicting failed.");
      // write dirty page back
      if (entry->Dirty) {
        auto batch = _client->BeginBatchTL(cur_thread_id);
#ifdef STAT
        stat::dirty_write.fetch_add(1);
#endif
        auto ret = writeToRemote(entry, batch, false);
        // writeToRemote(old_entry, batch, false);
        // old_entry->Dirty = false;
        // LOG_ASSERT(ret == 0, "rdma write failed.");
        *batch_ret = batch;
        entry->Dirty = false;
        _buffer_pool->PinPage(entry);
        _buffer_pool->InsertPage(entry, meta->PageId(), slab_class);
        _allocing_pages[al_index][slab_class] = entry;
        _allocing_tail[al_index][slab_class] = meta;
        LOG_ASSERT(_allocing_tail[al_index][slab_class] != nullptr, "tail should not be null");
        LOG_ASSERT(_allocing_tail[al_index][slab_class]->Prev() == nullptr, "prev should null");

        _buffer_pool->UnpinPage(old_entry);
        old_meta->UnPin();
        return entry;
      }
      _buffer_pool->PinPage(entry);
      _buffer_pool->InsertPage(entry, meta->PageId(), slab_class);
      _buffer_pool->UnpinPage(old_entry);
      old_meta->UnPin();
      _allocing_pages[al_index][slab_class] = entry;
      _allocing_tail[al_index][slab_class] = meta;
      LOG_ASSERT(_allocing_tail[al_index][slab_class] != nullptr, "tail should not be null");
      return entry;
    }
    _buffer_pool->PinPage(entry);
    _buffer_pool->UnpinPage(old_entry);
    old_meta->UnPin();
    _allocing_pages[al_index][slab_class] = entry;
    _allocing_tail[al_index][slab_class] = meta;
    LOG_ASSERT(_allocing_tail[al_index][slab_class] != nullptr, "tail should not be null");

    return entry;
  }
  // 说明后面有挂载的其他page，将其pin住
  meta->Pin();
  meta->al_index = al_index;
  LOG_ASSERT(meta->IsMounted(), "meta should be mounted");
  LOG_ASSERT(!meta->Full(), "invalid allocing page.");
  global_page_manager->Unmount(old_meta);
  _buffer_pool->UnpinPage(old_entry);
  old_meta->UnPin();
  PageId page_id = meta->PageId();
  entry = _buffer_pool->Lookup(page_id, true);
  if (entry == nullptr) {
    // replacement
    entry = replacement(page_id, slab_class, true);
  }
  _buffer_pool->PinPage(entry);
  _allocing_pages[al_index][slab_class] = entry;
  return entry;
}

constexpr int PER_THREAD_PAGE_NUM = kPoolSize / kPageSize / kThreadNum;
PageEntry *Pool::replacement(PageId page_id, uint8_t slab_class, bool writer) {
  auto batch = _client->BeginBatchTL(cur_thread_id);
  std::vector<PageEntry *> victims;
  std::vector<PageId> prefetch_page_ids;
  PageEntry *victim = _buffer_pool->Evict();
  victims.emplace_back(victim);
  prefetch_page_ids.emplace_back(page_id);
  int prefetch_num;
  for (prefetch_num = 1; prefetch_num < kPrefetchPageNum; prefetch_num++) {
    page_id++;
    if (page_id % PER_THREAD_PAGE_NUM == 0 || _buffer_pool->Lookup(page_id) != nullptr) break;
    victim = _buffer_pool->Evict();
    victims.emplace_back(victim);
    prefetch_page_ids.emplace_back(page_id);
  }

  // 预取
  for (int i = 0; i < prefetch_num; i++) {
    victim = victims[i];
    page_id = prefetch_page_ids[i];
    if (victim->Dirty) {
      writeToRemote(victim, batch, false);
      victim->Dirty = false;
    }
    readFromRemote(victim, page_id, batch);
  }
  batch->FinishBatchTL();

  PageMeta *meta;
  for (int i = 0; i < prefetch_num; i++) {
    victim = victims[i];
    page_id = prefetch_page_ids[i];
    meta = global_page_manager->Page(page_id);
    _buffer_pool->InsertPage(victim, page_id, meta->SlabClass());
  }

  return victims[0];
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

  int al_index = cur_thread_id;
  PageEntry *page;
  int off;
  PageMeta *meta;
  RDMAManager::Batch *batch = nullptr;
  page = _allocing_pages[al_index][slab_class];
  meta = global_page_manager->Page(page->PageId());
  if (UNLIKELY(meta->Full())) {
    page = mountNewPage(slab_class, hash, &batch, al_index);
    meta = global_page_manager->Page(page->PageId());
  }
  // modify bitmap
  off = meta->SetFirstFreePos();
  if (batch != nullptr) {
    batch->FinishBatchTL();
  }

  LOG_ASSERT(off != -1, "set bitmap failed.");
  Addr addr = AddrParser::GenAddrFrom(meta->PageId(), off);
  slot->SetAddr(addr);

  // copy data
  my_memcpy((void *)(page->Data() + off * val.size()), val.data(), val.size());
  if (!page->Dirty) page->Dirty = true;
  _buffer_pool->Release(page);

  return true;
}

int Pool::writeToRemote(PageEntry *entry, RDMAManager::Batch *batch, bool use_net_buffer) {
  // LOG_INFO("pageid %d writeToRemote", entry->PageId());
  uint32_t block = AddrParser::GetBlockFromPageId(entry->PageId());
  uint32_t block_off = AddrParser::GetBlockOffFromPageId(entry->PageId());
  LOG_DEBUG("write to block %d off %d", block, block_off);
  const MemoryAccess &access = _access_table[block];
  batch->RemoteWrite(entry->Data(), _buffer_pool->MR(entry->MRID())->lkey, kPageSize,
                     access.addr + kPageSize * block_off, access.rkey);
  return 0;
}

int Pool::readFromRemote(PageEntry *entry, PageId page_id, RDMAManager::Batch *batch) {
  uint32_t block = AddrParser::GetBlockFromPageId(page_id);
  uint32_t block_off = AddrParser::GetBlockOffFromPageId(page_id);
  const MemoryAccess &access = _access_table[block];
  return batch->RemoteRead(entry->Data(), _buffer_pool->MR(entry->MRID())->lkey, kPageSize,
                           access.addr + kPageSize * block_off, access.rkey);
}

}  // namespace kv