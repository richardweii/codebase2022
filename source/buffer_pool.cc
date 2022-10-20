#include "buffer_pool.h"
#include <list>
#include <thread>
#include "config.h"
#include "hash_index.h"
#include "util/logging.h"

namespace kv {
class ClockReplacer {
 public:
  static constexpr uint8_t REF = 0x80;
  static constexpr uint8_t UNREF = 0x7f;

  explicit ClockReplacer(size_t frame_num) : frame_num_(frame_num) {
    per_thread_frame_num = frame_num_ / kThreadNum;
    for (int i = 0; i < kThreadNum; i++) {
      frames_[i] = new Frame[per_thread_frame_num];
      hand_[i] = 0;
    }
    for (int i = 0; i < kThreadNum; i++) {
      for (int j = 0; j < per_thread_frame_num; j++) {
        frames_[i][j]._frame = i * per_thread_frame_num + j;
        free_list_[i].push_back(frames_[i][j]._frame);
      }
    }
  }
  ~ClockReplacer() {
    for (int i = 0; i < kThreadNum; i++) delete[] frames_[i];
  }

  // thread-safe
  bool Victim(FrameId *frame_id) {
    FrameId fid;
    assert(frame_id != nullptr);
    fid = this->pop();
    LOG_ASSERT(fid < per_thread_frame_num, "fid %d", fid);
    frames_[cur_thread_id][fid]._victim = true;
    LOG_ASSERT(!frames_[cur_thread_id][fid]._pin, "frame_id %d is pinned", *frame_id);
    *frame_id = cur_thread_id * per_thread_frame_num + fid;
    return *frame_id != INVALID_FRAME_ID;
  }

  // thread-safe
  bool GetFrame(FrameId *frame_id, int tid) {
    FrameId fid;
    if (free_list_[tid].empty()) {
      return false;
    }
    *frame_id = free_list_[tid].front();
    free_list_[tid].pop_front();
    return true;
  }

  // thread-safe
  void Pin(FrameId frame_id) {
    int tid = frame_id / per_thread_frame_num;
    int off = frame_id % per_thread_frame_num;
    LOG_ASSERT(!frames_[tid][off]._pin, "frame_id %d is pinned", frame_id);
    frames_[tid][off]._pin = true;
  }

  // thread-safe
  void Unpin(FrameId frame_id) {
    int tid = frame_id / per_thread_frame_num;
    int off = frame_id % per_thread_frame_num;
    assert(frames_[tid][off]._pin);
    frames_[tid][off]._pin = false;
  }

  void Ref(FrameId frame_id) {
    int tid = frame_id / per_thread_frame_num;
    int off = frame_id % per_thread_frame_num;
    if (frames_[tid][off]._victim) frames_[tid][off]._victim = false;
    if (!frames_[tid][off]._ref) frames_[tid][off]._ref = true;
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
    int ret = hand_[cur_thread_id];
    hand_[cur_thread_id] = (hand_[cur_thread_id] + 1) % per_thread_frame_num;
    LOG_ASSERT(ret < per_thread_frame_num, "fid %d", ret);
    return ret;
  }

  FrameId pop() {
    while (true) {
      if (!frames_[cur_thread_id][hand_[cur_thread_id]]._pin && !frames_[cur_thread_id][hand_[cur_thread_id]]._ref &&
          !frames_[cur_thread_id][hand_[cur_thread_id]]._victim) {
        break;
      } else if (!frames_[cur_thread_id][hand_[cur_thread_id]]._pin) {
        frames_[cur_thread_id][hand_[cur_thread_id]]._ref = false;
      }
      walk();
    }
    return walk();
  }
  int hand_[kThreadNum];
  size_t frame_num_;
  Frame *frames_[kThreadNum];
  std::list<FrameId> free_list_[kThreadNum];
  SpinLock _lock;
  int per_thread_frame_num;
};

struct Slot {
  PageId _page_id = INVALID_PAGE_ID;
  FrameId _frame = INVALID_FRAME_ID;
  Slot *_next = nullptr;
};

// thread-safe
class FrameHashTable {
 public:
  FrameHashTable(size_t size) { _slots = new Slot[size]; }

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

BufferPool::BufferPool(size_t buffer_pool_size) : _buffer_pool_size(buffer_pool_size) {
  size_t page_num = buffer_pool_size / kPageSize;
  _per_thread_page_num = page_num / kThreadNum;
  _pages = (PageData *)aligned_alloc(4096, sizeof(PageData) * page_num);
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
  std::vector<std::thread> threads;
  for (int t = 0; t < kPoolMrBlockNum; t++) {
    threads.emplace_back(
        [&](int i) {
          _mr[i] = ibv_reg_mr(pd, (char *)_pages + per_mr_bp_sz * i, per_mr_bp_sz, RDMA_MR_FLAG);
          if (_mr[i] == nullptr) {
            LOG_FATAL("Register %lu memory failed.", per_mr_bp_sz);
          }
        },
        t);
  }
  for (auto &th : threads) {
    th.join();
  }

  return true;
}

PageEntry *BufferPool::FetchNew(PageId page_id, uint8_t slab_class, int tid) {
  FrameId fid;
  if (_replacer->GetFrame(&fid, tid)) {
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
  LOG_ASSERT(fid >= 0 && fid < _buffer_pool_size / kPageSize, "invaild fid %d", fid);

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