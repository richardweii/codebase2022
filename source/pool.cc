#include "pool.h"
#include <infiniband/verbs.h>
#include <cassert>
#include <cstdint>
#include <cstdlib>
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

Pool::Pool(uint8_t shard, RDMAClient *client, MemoryAccess *global_rdma_access)
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
    for (int j = 0; j < kAllocingListShard; j++) {
      PageMeta *page = global_page_manager->AllocNewPage(i);
      _allocing_tail[j][i] = page;
      _allocing_pages[j][i] = _buffer_pool->FetchNew(page->PageId(), i);
      _buffer_pool->PinPage(_allocing_pages[j][i]);
      _allocing_tail[j][i]->Pin();
    }
  }

  // mr net buffer
  _net_buffer_mr = ibv_reg_mr(_client->Pd(), _net_buffer, sizeof(NetBuffer) * kThreadNum, RDMA_MR_FLAG);
  if (_net_buffer_mr == nullptr) {
    LOG_FATAL("Register Net Buffer Failed.");
  }

  // send net buffer meta to remote
  MessageBlock *block;

  NetBufferInitReq req;
  req.type = MSG_NET_BUFFER;
  req.addr = (uintptr_t)_net_buffer_mr->addr;
  req.rkey = _net_buffer_mr->rkey;
  _client->RPCSend(req, block);

  NetBufferInitResponse resp;
  auto rc = _client->RPCRecv(resp, block);
  assert(rc == 0);

  for (int i = kSlabSizeMin; i <= kSlabSizeMax; i++) {
    _max_slot_num[i] = kPageSize / kSlabSize / i;
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
  PageEntry *victim = _replacement_sgfl.Do(page_id, page_id, _replacement, page_id, meta->SlabClass(), false);
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
      if (AddrParser::Off(addr) + 1 == _max_slot_num[meta->SlabClass()]) {
        asyncFlushPage(entry);
      }
      _buffer_pool->Release(entry);
      return true;
    }

    // cache miss
    // stat::replacement++;
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

  if (meta->al_index == -1) {
    meta->al_index = cur_thread_id;
  }
  int al_index = meta->al_index;
  // int al_index = (hash >> 24) % kAllocingListShard;
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

  meta->al_index = cur_thread_id;
  int al_index = meta->al_index;
  // int al_index = (hash >> 24) % kAllocingListShard;
  LOG_ASSERT(al_index >= 0 && al_index <= 15, "bound error");
  allocingListWLock(al_index, meta->SlabClass());
  meta->ClearPos(AddrParser::Off(addr));
  global_page_manager->Mount(&_allocing_tail[al_index][meta->SlabClass()], meta);
  if (!meta->IsPined() && meta->Empty()) {
    if (_allocing_tail[al_index][meta->SlabClass()]->PageId() == page_id) {
      _allocing_tail[al_index][meta->SlabClass()] = meta->Prev();
      LOG_ASSERT(_allocing_tail[al_index][meta->SlabClass()] != nullptr, "tail should not be null");
      // LOG_DEBUG("[shard %d] set class %d tail page %d", _shard, meta->SlabClass(), meta->Prev()->PageId());
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
  al_index = meta->al_index;
  LOG_ASSERT(al_index >= 0 && al_index <= 15, "bound error");
  allocingListWLock(al_index, slab_class);
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
  allocingListWUnlock(al_index, slab_class);

  my_memcpy((char *)(page->Data() + val.size() * off), val.data(), val.size());
  if (!page->Dirty) page->Dirty = true;
  _buffer_pool->Release(page);

  LOG_ASSERT(off != -1, "set bitmap failed.");
  addr = AddrParser::GenAddrFrom(meta->PageId(), off);
  slot->SetAddr(addr);
}

PageEntry *Pool::mountNewPage(uint8_t slab_class, uint32_t hash, RDMAManager::Batch **batch_ret, int tid) {
  int al_index;
  if (tid != -1) {
    al_index = tid;
  } else {
    al_index = cur_thread_id;
  }
  // int al_index = (hash >> 24) % kAllocingListShard;

  PageEntry *old_entry;
  PageMeta *old_meta;

  old_entry = _allocing_pages[al_index][slab_class];
  _allocing_pages[al_index][slab_class] = nullptr;
  old_meta = global_page_manager->Page(old_entry->PageId());
  asyncFlushPage(old_entry);

  PageMeta *meta = old_meta->Next();
  PageEntry *entry = nullptr;
  if (meta == nullptr) {
    assert(old_meta->Next() == nullptr);
    assert(old_meta->Prev() == nullptr);
    // allocing list is empty, need alloc new page
    meta = global_page_manager->AllocNewPage(slab_class);
    meta->Pin();
    entry = _buffer_pool->FetchNew(meta->PageId(), slab_class);
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
        // stat::replacement++;
        auto ret = writeToRemote(entry, batch, false);
        // LOG_ASSERT(ret == 0, "rdma write failed.");
        if (ret == 1) {
          *batch_ret = nullptr;
        } else {
          *batch_ret = batch;
        }
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
  LOG_ASSERT(meta->IsMounted(), "meta should be mounted");
  LOG_ASSERT(!meta->Full(), "invalid allocing page.");
  global_page_manager->Unmount(old_meta);
  _buffer_pool->UnpinPage(old_entry);
  old_meta->UnPin();
  PageId page_id = meta->PageId();
  entry = _buffer_pool->Lookup(page_id, true);
  if (entry == nullptr) {
    // replacement
    entry = _replacement_sgfl.Do(page_id, page_id, _replacement, page_id, slab_class, true);
  }
  _buffer_pool->PinPage(entry);
  _allocing_pages[al_index][slab_class] = entry;
  return entry;
}

PageEntry *Pool::replacement(PageId page_id, uint8_t slab_class, bool writer) {
  // miss
#ifdef STAT
  stat::replacement.fetch_add(1);
#endif
  // recheck
  // 这里必须要recheck，因为在调用replacement之前的check中，虽然这个page_id不在本地，但是由于没有锁的保证，所以可能在你check完之后它又在了
  // 如果不recheck，那么会从远端读取数据，而本地的那个可能发生更改，那么就会导致远端的旧数据覆盖本地写入的新数据，导致读取发生错误
  PageEntry *entry = _buffer_pool->Lookup(page_id, writer);
  if (entry != nullptr) {
    return entry;
  }

  PageEntry *victim = _buffer_pool->Evict();
  auto batch = _client->BeginBatchTL(cur_thread_id);
  if (victim->Dirty) {
#ifdef STAT
    stat::dirty_write.fetch_add(1);
#endif
    auto ret = writeToRemote(victim, batch, false);
    // LOG_ASSERT(ret == 0, "write page %d to remote failed.", victim->PageId());
    victim->Dirty = false;
  }
  auto ret = readFromRemote(victim, page_id, batch);
  ret = batch->FinishBatchTL();
  if (open_compress) {
    // 解压缩
    LZ4_decompress_safe(_buffer_pool->compress_page_buff[cur_thread_id + kThreadNum].data, victim->Data(),
                        _buffer_pool->pg_com_szs[page_id], kPageSize);
  }
  // delete batch;
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

  int al_index = cur_thread_id;
  // int al_index = (hash >> 24) % kAllocingListShard;
  PageEntry *page;
  int off;
  PageMeta *meta;
  RDMAManager::Batch *batch = nullptr;
  allocingListWLock(al_index, slab_class);
  page = _allocing_pages[al_index][slab_class];
  meta = global_page_manager->Page(page->PageId());
  if (UNLIKELY(meta->Full())) {
    page = mountNewPage(slab_class, hash, &batch, al_index);
    meta = global_page_manager->Page(page->PageId());
  }
  meta->al_index = cur_thread_id;
  // modify bitmap
  off = meta->SetFirstFreePos();
  if (batch != nullptr) {
    batch->FinishBatchTL();
  }
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

int Pool::writeToRemote(PageEntry *entry, RDMAManager::Batch *batch, bool use_net_buffer) {
  // LOG_INFO("pageid %d writeToRemote", entry->PageId());
  uint32_t block = AddrParser::GetBlockFromPageId(entry->PageId());
  uint32_t block_off = AddrParser::GetBlockOffFromPageId(entry->PageId());
  LOG_DEBUG("write to block %d off %d", block, block_off);
  const MemoryAccess &access = _access_table[block];
  if (use_net_buffer &&
      _net_buffer[cur_thread_id].produce(entry->Data(), access.addr + kPageSize * block_off, access.lkey)) {
    // net buff有空间
    // LOG_INFO("pageid %d 命中net buff", entry->PageId());
    stat::hit_net_buffer++;
    return 1;
  }
  if (use_net_buffer) stat::miss_net_buffer++;
  // LOG_INFO("[%d] pageid %d 未命中 net buff", cur_thread_id, entry->PageId());
RETRY:
  if (open_compress) {
    // 先压缩
    size_t com_size =
        LZ4_compress_fast(entry->Data(), _buffer_pool->compress_page_buff[cur_thread_id].data, kPageSize, kPageSize, 1);
    _buffer_pool->pg_com_szs[entry->PageId()] = com_size;
    if (com_size == 0 || ((kPageSize * 1.0) / com_size) < 1.5) {
      open_compress = false;
      LOG_INFO("close compress");
      goto RETRY;
    }

    batch->RemoteWrite(_buffer_pool->compress_page_buff[cur_thread_id].data, _buffer_pool->CompressMR()->lkey, com_size,
                       access.addr + kPageSize * block_off, access.rkey);
    return 0;
  } else {
    batch->RemoteWrite(entry->Data(), _buffer_pool->MR(entry->MRID())->lkey, kPageSize,
                       access.addr + kPageSize * block_off, access.rkey);
    return 0;
  }
}

int Pool::readFromRemote(PageEntry *entry, PageId page_id, RDMAManager::Batch *batch) {
  uint32_t block = AddrParser::GetBlockFromPageId(page_id);
  uint32_t block_off = AddrParser::GetBlockOffFromPageId(page_id);
  const MemoryAccess &access = _access_table[block];
  if (open_compress) {
    // 读取到的是压缩后的数据
    return batch->RemoteRead(_buffer_pool->compress_page_buff[cur_thread_id + kThreadNum].data,
                             _buffer_pool->CompressMR()->lkey, _buffer_pool->pg_com_szs[page_id],
                             access.addr + kPageSize * block_off, access.rkey);
  } else {
    return batch->RemoteRead(entry->Data(), _buffer_pool->MR(entry->MRID())->lkey, kPageSize,
                             access.addr + kPageSize * block_off, access.rkey);
  }
}

void Pool::asyncFlushPage(PageEntry *entry) {
  // stat::async_flush++;
  auto dirtyFlushBatch = _client->DirtyFlushBatch(cur_thread_id);
  if (dirtyFlushBatch->BatchNum() >= 30) {
    dirtyFlushBatch->PollCQ(15);
  }
  uint32_t block = AddrParser::GetBlockFromPageId(entry->PageId());
  uint32_t block_off = AddrParser::GetBlockOffFromPageId(entry->PageId());
  const MemoryAccess &access = _access_table[block];
  dirtyFlushBatch->RemoteWrite(entry->Data(), _buffer_pool->MR(entry->MRID())->lkey, kPageSize,
                               access.addr + kPageSize * block_off, access.rkey);
  entry->Dirty = false;
}
}  // namespace kv