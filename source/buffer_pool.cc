#include "buffer_pool.h"
#include <list>
#include "config.h"
#include "hash_index.h"

namespace kv {
class ClockReplacer {
 public:
  static constexpr uint8_t REF = 0x80;
  static constexpr uint8_t UNREF = 0x7f;

  explicit ClockReplacer(size_t frame_num) : frame_num_(frame_num) {
    frames_ = new Frame[frame_num];
    for (size_t i = 0; i < frame_num; i++) {
      frames_[i]._frame = i;
      free_list_.push_back(i);
    }
  }
  ~ClockReplacer() { delete[] frames_; }

  // thread-safe
  bool Victim(FrameId *frame_id) {
    assert(frame_id != nullptr);
    _lock.Lock();
    *frame_id = this->pop();
    frames_[*frame_id]._victim = true;
    _lock.Unlock();
    LOG_ASSERT(!frames_[*frame_id]._pin, "frame_id %d is pinned", *frame_id);
    return *frame_id != INVALID_FRAME_ID;
  }

  // thread-safe
  bool GetFrame(FrameId *frame_id) {
    _lock.Lock();
    if (free_list_.empty()) {
      _lock.Unlock();
      return false;
    }
    *frame_id = free_list_.front();
    free_list_.pop_front();
    _lock.Unlock();
    return true;
  }

  // thread-safe
  void Pin(FrameId frame_id) {
    LOG_ASSERT(!frames_[frame_id]._pin, "frame_id %d is pinned", frame_id);
    frames_[frame_id]._pin = true;
  }

  // thread-safe
  void Unpin(FrameId frame_id) {
    assert(frames_[frame_id]._pin);
    frames_[frame_id]._pin = false;
  }

  void Ref(FrameId frame_id) {
    if (frames_[frame_id]._victim) frames_[frame_id]._victim = false;
    if (!frames_[frame_id]._ref) frames_[frame_id]._ref = true;
  }

 private:
  struct Frame {
    Frame() = default;
    FrameId _frame = INVALID_FRAME_ID;
    bool _pin = false;
    bool _ref = false;
    bool _victim = true;
  };

  int walk() {
    int ret = hand_;
    hand_ = (hand_ + 1) % frame_num_;
    return ret;
  }

  FrameId pop() {
    while (true) {
      uint8_t tmp = 0;
      if (!frames_[hand_]._pin && !frames_[hand_]._ref && !frames_[hand_]._victim) {
        break;
      } else if (!frames_[hand_]._pin) {
        frames_[hand_]._ref = false;
      }
      walk();
    }
    return walk();
  }
  int hand_ = 0;
  size_t frame_num_;
  Frame *frames_ = nullptr;
  std::list<FrameId> free_list_;
  SpinLock _lock;
};

struct Slot {
  PageId _page_id = INVALID_PAGE_ID;
  uint32_t _frame = INVALID_FRAME_ID;
  Slot *_next = nullptr;
};

// thread-safe
class FrameHashTable {
 public:
  FrameHashTable(size_t size) {
    _slots = new Slot[size];
  }

  FrameId Find(PageId page_id) { return _slots[page_id]._frame; }

  void Insert(PageId page_id, FrameId frame) { _slots[page_id]._frame = frame; }

  bool Remove(PageId page_id, FrameId frame) {
    if (_slots[page_id]._frame == INVALID_FRAME_ID) {
      return false;
    }
    _slots[page_id]._frame = INVALID_FRAME_ID;
    return true;
  }

  ~FrameHashTable() { delete[] _slots; }

 private:
  Slot *_slots;
};
constexpr int a = kPoolSize / kPageSize;
BufferPool::BufferPool(size_t buffer_pool_size, uint8_t shard) : _buffer_pool_size(buffer_pool_size), _shard(shard) {
  size_t page_num = buffer_pool_size / kPageSize;
  _pages = new PageData[page_num];
  _hash_table = new FrameHashTable(kPoolSize / kPageSize);
  _replacer = new ClockReplacer(page_num);
  _entries = new PageEntry[page_num];
  int per_wr_page_num = page_num / kPoolMrBlockNum;
  for (size_t i = 0; i < page_num; i++) {
    _entries[i].mr_id = i / per_wr_page_num;
    _entries[i]._frame_id = i;
    _entries[i]._data = &_pages[i];
  }
}

BufferPool::~BufferPool() {
  for (int i = 0; i < kPoolMrBlockNum; i++) {
    if (ibv_dereg_mr(_mr[i])) {
      perror("ibv_derge_mr failed.");
      LOG_FATAL("ibv_derge_mr failed.");
    }
  }

  delete[] _pages;
  delete[] _entries;
  delete _hash_table;
  delete _replacer;
}

bool BufferPool::Init(ibv_pd *pd) {
  size_t per_mr_bp_sz = _buffer_pool_size / kPoolMrBlockNum;
  for (int i = 0; i < kPoolMrBlockNum; i++) {
    _mr[i] = ibv_reg_mr(pd, (char *)_pages + per_mr_bp_sz * i, per_mr_bp_sz, RDMA_MR_FLAG);
    if (_mr[i] == nullptr) {
      LOG_FATAL("Register %lu memory failed.", per_mr_bp_sz);
      return false;
    }
  }

  compress_page_buff_mr = ibv_reg_mr(pd, compress_page_buff, sizeof(compress_page_buff), RDMA_MR_FLAG);
  if (compress_page_buff_mr == nullptr) {
    LOG_FATAL("Register %lu memory failed.", sizeof(compress_page_buff));
    return false;
  }

  return true;
}

PageEntry *BufferPool::FetchNew(PageId page_id, uint8_t slab_class) {
  FrameId fid;
  if (_replacer->GetFrame(&fid)) {
    PageEntry *new_entry = &_entries[fid];
    new_entry->_page_id = page_id;
    new_entry->_slab_class = slab_class;
    _hash_table->Insert(page_id, fid);
    // LOG_DEBUG("[shard %d] fetch new page %d", _shard, page_id);
    return new_entry;
  }
  return nullptr;
}

PageEntry *BufferPool::Lookup(PageId page_id, bool writer) {
  FrameId fid;
  fid = _hash_table->Find(page_id);
  if (fid == INVALID_FRAME_ID) {
    return nullptr;
  }

  _replacer->Ref(fid);
  return &_entries[fid];
}

void BufferPool::Release(PageEntry *entry) { _replacer->Ref(entry->_frame_id); }

void BufferPool::InsertPage(PageEntry *page, PageId page_id, uint8_t slab_class) {
  page->_page_id = page_id;
  page->_slab_class = slab_class;
  _hash_table->Insert(page_id, page->_frame_id);
  _replacer->Ref(page->_frame_id);
}

PageEntry *BufferPool::Evict() {
  FrameId fid;
  auto succ = _replacer->Victim(&fid);
  assert(succ);
  PageEntry *victim = &_entries[fid];

  // remove from old hash table
  _hash_table->Remove(victim->_page_id, victim->_frame_id);
  return victim;
}

PageEntry *BufferPool::EvictBatch(int batch_size, std::vector<PageEntry *> *pages) {
  // TODO: batching evict.
  LOG_FATAL("not implemented.");
  return nullptr;
}

void BufferPool::PinPage(PageEntry *entry) { _replacer->Pin(entry->_frame_id); }
void BufferPool::UnpinPage(PageEntry *entry) { _replacer->Unpin(entry->_frame_id); }
}  // namespace kv