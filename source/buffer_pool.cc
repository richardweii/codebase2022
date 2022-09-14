#include "buffer_pool.h"
#include <list>
#include "hash_table.h"

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

  // not thread-safe
  bool Victim(FrameId *frame_id) {
    assert(frame_id != nullptr);
    *frame_id = this->pop();
    LOG_ASSERT(!frames_[*frame_id].pin_, "frame_id %d is pinned", *frame_id);
    return *frame_id != INVALID_FRAME_ID;
  }

  bool GetFrame(FrameId *frame_id) {
    if (free_list_.empty()) {
      return false;
    }
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }

  // thread-safe
  void Pin(FrameId frame_id) {
    LOG_ASSERT(!frames_[frame_id].pin_, "frame_id %d is pinned", frame_id);
    frames_[frame_id].pin_ = true;
  }

  // thread-safe
  void Unpin(FrameId frame_id) {
    assert(frames_[frame_id].pin_);
    frames_[frame_id].pin_ = false;
  }

  void Ref(FrameId frame_id) {
    if (frames_[frame_id].victim_) frames_[frame_id].victim_ = false;
    if (!frames_[frame_id].ref_) frames_[frame_id].ref_ = true;
  }

 private:
  struct Frame {
    Frame() = default;
    FrameId _frame = INVALID_FRAME_ID;
    bool pin_ = false;
    bool ref_ = false;
    bool victim_ = true;
  };

  int walk() {
    int ret = hand_;
    hand_ = (hand_ + 1) % frame_num_;
    return ret;
  }

  FrameId pop() {
    while (true) {
      uint8_t tmp = 0;
      if (!frames_[hand_].pin_ && !frames_[hand_].ref_ && !frames_[hand_].victim_) {
        break;
      } else if (!frames_[hand_].pin_) {
        frames_[hand_].ref_ = false;
      }
      walk();
    }
    return walk();
  }
  int hand_ = 0;
  size_t frame_num_;
  Frame *frames_ = nullptr;
  std::list<FrameId> free_list_;
};

class FrameHashTable {
 public:
  FrameHashTable(size_t size) {
    int logn = 0;
    while (size >= 2) {
      size /= 2;
      logn++;
    }
    _size = PrimeList[logn];
    _slots = new Slot[_size];
    // _slot_latch = new SpinLatch[_size];
    // counter_ = new uint8_t[_size]{0};
  }

  FrameId Find(PageId page_id) {
    uint32_t index = page_id % _size;

    Slot *slot = &_slots[index];
    // _slot_latch[index].RLock();
    // defer { _slot_latch[index].RUnlock(); };

    if (slot->_page_id == INVALID_PAGE_ID) {
      return INVALID_FRAME_ID;
    }

    while (slot != nullptr) {
      if (page_id == slot->_page_id) {
        return slot->_frame;
      }
      slot = slot->_next;
    }
    return INVALID_FRAME_ID;
  }

  void Insert(PageId page_id, FrameId frame) {
    uint32_t index = page_id % _size;
    Slot *slot = &_slots[index];

    // _slot_latch[index].WLock();
    // defer { _slot_latch[index].WUnlock(); };
    if (slot->_page_id == INVALID_PAGE_ID) {
      slot->_page_id = page_id;
      slot->_frame = frame;
      // counter_[index]++;
      return;
    }

    // find
    while (slot != nullptr) {
      if (page_id == slot->_page_id) {
        // duplicate
        return;
      }
      slot = slot->_next;
    }

    // insert into head
    slot = new Slot();
    slot->_page_id = page_id;
    slot->_frame = frame;
    slot->_next = _slots[index]._next;
    _slots[index]._next = slot;
    // counter_[index]++;
  }

  bool Remove(PageId page_id, FrameId frame) {
    uint32_t index = page_id % _size;

    Slot *slot = &_slots[index];

    // _slot_latch[index].WLock();
    // defer { _slot_latch[index].WUnlock(); };

    if (slot->_page_id == INVALID_PAGE_ID) {
      return false;
    }

    // head
    if (page_id == slot->_page_id) {
      if (slot->_next != nullptr) {
        Slot *tmp = slot->_next;
        slot->_page_id = tmp->_page_id;
        slot->_frame = tmp->_frame;
        slot->_next = tmp->_next;
        delete tmp;
      } else {
        slot->_page_id = INVALID_PAGE_ID;
        slot->_frame = INVALID_FRAME_ID;
        slot->_next = nullptr;
      }
      // counter_[index]--;
      return true;
    }

    // find
    Slot *front = slot;
    while (slot != nullptr) {
      if (page_id == slot->_page_id) {
        front->_next = slot->_next;
        delete slot;
        // counter_[index]--;
        return true;
      }
      front = slot;
      slot = slot->_next;
    }
    // cannot find
    return false;
  }

  ~FrameHashTable() { delete[] _slots; }

 private:
  struct Slot {
    PageId _page_id = INVALID_PAGE_ID;
    uint32_t _frame;
    Slot *_next = nullptr;
  };
  Slot *_slots;
  SpinLatch *_slot_latch;
  size_t _size;
};

BufferPool::BufferPool(size_t buffer_pool_size, uint8_t shard) : _buffer_pool_size(buffer_pool_size), _shard(shard) {
  size_t page_num = buffer_pool_size / kPageSize;
  _pages = new PageData[page_num];
  _hash_table = new FrameHashTable(page_num);
  _replacer = new ClockReplacer(page_num);
  _entries = new PageEntry[page_num];
  for (size_t i = 0; i < page_num; i++) {
    _entries[i]._frame_id = i;
    _entries[i]._data = &_pages[i];
  }
}

BufferPool::~BufferPool() {
  if (ibv_dereg_mr(_mr)) {
    perror("ibv_derge_mr failed.");
    LOG_ERROR("ibv_derge_mr failed.");
  }
  // LOG_INFO("cache hash table");
  // LOG_INFO("cache lookup conflict %u", lookup_count_);
  // hash_table_->PrintCounter();
  delete[] _pages;
  delete[] _entries;
  delete _hash_table;
  delete _replacer;
}

bool BufferPool::Init(ibv_pd *pd) {
  _mr = ibv_reg_mr(pd, _pages, _buffer_pool_size, RDMA_MR_FLAG);
  if (_mr == nullptr) {
    LOG_ERROR("Register %lu memory failed.", _buffer_pool_size);
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

PageEntry *BufferPool::Lookup(PageId page_id) {
  // _latch.RLock();
  // defer { _latch.RUnlock(); };
  while (true) {
    auto fid = _hash_table->Find(page_id);
    if (fid == INVALID_FRAME_ID) {
      return nullptr;
    }

    if (!(page_id == _entries[fid]._page_id)) {
      // lookup_count_++;
      continue;
    }

    LOG_ASSERT(page_id == _entries[fid]._page_id, "Unmatched page. expect %u, got %u", page_id, _entries[fid]._page_id);
    _replacer->Ref(fid);
    // LOG_DEBUG("[shard %d] lookup page %d", _shard, page_id);
    return &_entries[fid];
  }
}

void BufferPool::Release(PageEntry *entry) { _replacer->Ref(entry->_frame_id); }

void BufferPool::InsertPage(PageEntry *page, PageId page_id, uint8_t slab_class) {
  page->_page_id = page_id;
  page->_slab_class = slab_class;
  _hash_table->Insert(page_id, page->_frame_id);
  _replacer->Ref(page->_frame_id);
}

PageEntry *BufferPool::Evict() {
  // _latch.WLock();
  // defer { _latch.WUnlock(); };
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