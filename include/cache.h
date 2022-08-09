#pragma once

#include <infiniband/verbs.h>
#include <sys/types.h>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <list>
#include <mutex>
#include <unordered_map>

#include "config.h"
#include "hash_table.h"
#include "rdma_client.h"
#include "util/logging.h"
#include "util/nocopy.h"
#include "util/rwlock.h"
#include "util/slice.h"

namespace kv {

class CacheLine {
 public:
  char *at(int index) {
    assert(index < kCacheValueNum);
    return &data_[index * kValueLength];
  }

 private:
  char data_[kCacheLineSize];
};

class CacheEntry {
 public:
  friend class Cache;
  kv::ID ID;
  bool Dirty = false;

  CacheLine *Data() const { return line_; }

 private:
  CacheEntry() = default;
  CacheLine *line_ = nullptr;
  int fid_ = INVALID_FRAME_ID;
};

class LRUReplacer {
 public:
  explicit LRUReplacer(size_t frame_num);
  ~LRUReplacer();
  bool Victim(FrameId *frame_id);
  void Pin(FrameId frame_id);
  void Unpin(FrameId frame_id);

 private:
  struct Frame {
    Frame() = default;
    FrameId frame_ = INVALID_FRAME_ID;
    uint8_t pin = 1;
    Frame *next = nullptr;
    Frame *front = nullptr;
  };

  /**
   * Insert a frame to frame_list, and add <frame_id, frame pointer> to frame_mapping.
   * @param frame_id frame ID add to list
   */
  void insertBack(FrameId frame_id);
  /**
   * Delete a frame from frame_list, and remove <frame_id, frame pointer> from frame_mapping.
   * If frame_id does not exist in frame_mapping, nothing will happen.
   * @param frame_id frame ID remove from list
   */
  void deleteFrame(FrameId frame_id);
  FrameId pop();

  size_t frame_num_;
  Frame *frame_list_head_ = nullptr;
  Frame *frame_list_tail_ = nullptr;
  Frame *frames_ = nullptr;
  std::list<FrameId> free_list_;
};

class ClockReplacer {
 public:
  static constexpr uint8_t REF = 0x80;
  static constexpr uint8_t UNREF = 0x7f;

  explicit ClockReplacer(size_t frame_num) : frame_num_(frame_num) {
    frames_ = new Frame[frame_num];
    for (size_t i = 0; i < frame_num; i++) {
      frames_[i].frame_ = i;
      free_list_.push_back(i);
    }
  }
  ~ClockReplacer() { delete[] frames_; }

  // not thread-safe
  bool Victim(FrameId *frame_id) {
    assert(frame_id != nullptr);
    *frame_id = this->pop();
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
    assert(!frames_[frame_id].pin_);
    frames_[frame_id].pin_ = true;
  }

  // thread-safe
  void Unpin(FrameId frame_id) {
    assert(frames_[frame_id].pin_);
    frames_[frame_id].pin_ = false;
  }

  void Ref(FrameId frame_id) {
    frames_[frame_id].ref_ = true;
  }

  // void UnRef(FrameId frame_id, bool victim) {
  //   frames_[frame_id].ref_ = true;
  //   if (victim) {
  //     frames_[frame_id].victim_ = false;
  //   }
  // }

 private:
  struct Frame {
    Frame() = default;
    FrameId frame_ = INVALID_FRAME_ID;
    bool pin_ = false;
    bool ref_;
  };

  int walk() {
    int ret = hand_;
    hand_ = (hand_ + 1) % frame_num_;
    return ret;
  }

  FrameId pop() {
    while (true) {
      uint8_t tmp = 0;
      if (!frames_[hand_].pin_ && !frames_[hand_].ref_) {
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
    size_ = PrimeList[logn];
    slots_ = new Slot[size_];
    // counter_ = new uint8_t[size_]{0};
  }

  FrameId Find(uint32_t addr) {
    uint32_t index = addr % size_;

    Slot *slot = &slots_[index];
    if (slot->addr_ == Identifier::INVALID_ID) {
      return INVALID_FRAME_ID;
    }

    while (slot != nullptr) {
      if (addr == slot->addr_) {
        return slot->frame_;
      }
      slot = slot->next_;
    }
    return INVALID_FRAME_ID;
  }

  void Insert(uint32_t addr, FrameId frame) {
    uint32_t index = addr % size_;
    Slot *slot = &slots_[index];
    if (slot->addr_ == Identifier::INVALID_ID) {
      slot->addr_ = addr;
      slot->frame_ = frame;
      // counter_[index]++;
      return;
    }

    // find
    while (slot != nullptr) {
      if (addr == slot->addr_) {
        // duplicate
        LOG_DEBUG("slot addr %x, addr %x", slot->addr_, addr);
        return;
      }
      slot = slot->next_;
    }

    // insert into head
    slot = new Slot();
    slot->addr_ = addr;
    slot->frame_ = frame;
    slot->next_ = slots_[index].next_;
    slots_[index].next_ = slot;
    // counter_[index]++;
  }

  bool Remove(uint32_t addr, FrameId frame) {
    uint32_t index = addr % size_;

    Slot *slot = &slots_[index];

    if (slot->addr_ == Identifier::INVALID_ID) {
      return false;
    }

    // head
    if (addr == slot->addr_) {
      if (slot->next_ != nullptr) {
        Slot *tmp = slot->next_;
        *slot = *slot->next_;
        delete tmp;
      } else {
        slot->addr_ = Identifier::INVALID_ID;
        slot->frame_ = INVALID_FRAME_ID;
        slot->next_ = nullptr;
      }
      // counter_[index]--;
      return true;
    }

    // find
    Slot *front = slot;
    while (slot != nullptr) {
      if (addr == slot->addr_) {
        front->next_ = slot->next_;
        delete slot;
        // counter_[index]--;
        return true;
      }
      front = slot;
      slot = slot->next_;
    }
    // cannot find
    return false;
  }

  ~FrameHashTable() { delete[] slots_; }

 private:
  struct Slot {
    uint32_t addr_ = Identifier::INVALID_ID;
    uint32_t frame_;
    Slot *next_ = nullptr;
  };
  Slot *slots_;
  size_t size_;
};

class Cache NOCOPYABLE {
 public:
  Cache(size_t cache_size) : cache_line_num_(cache_size / kCacheLineSize) {
    lines_ = new CacheLine[cache_line_num_];
    entries_ = new CacheEntry[cache_line_num_];
    for (size_t i = 0; i < cache_line_num_; i++) {
      entries_[i].line_ = &lines_[i];
      entries_[i].fid_ = i;
    }
    replacer_ = new ClockReplacer(cache_line_num_);
    hash_table_ = new FrameHashTable(cache_line_num_);
  }

  bool Init(ibv_pd *pd) {
    mr_ = ibv_reg_mr(pd, lines_, cache_line_num_ * kCacheLineSize, RDMA_MR_FLAG);
    if (mr_ == nullptr) {
      LOG_ERROR("Register %lu memory failed.", cache_line_num_ * kCacheLineSize);
      return false;
    }
    return true;
  }

  ~Cache() {
    if (ibv_dereg_mr(mr_)) {
      perror("ibv_derge_mr failed.");
      LOG_ERROR("ibv_derge_mr failed.");
    }
    // LOG_INFO("cache hash table");
    // LOG_INFO("cache lookup conflict %u", lookup_count_);
    // hash_table_->PrintCounter();
    delete[] lines_;
    delete hash_table_;
    delete replacer_;
  }

  // return a cacheHandle used to write new data to it, if old data is dirty, need to write to remote first
  CacheEntry *Insert(ID addr);

  void Release(CacheEntry *entry);

  CacheEntry *Lookup(ID addr);

  ibv_mr *MR() const { return mr_; }

  void Pin(CacheEntry *entry) { replacer_->Pin(entry->fid_); }
  void UnPin(CacheEntry *entry) { replacer_->Unpin(entry->fid_); }

 private:
  size_t cache_line_num_;
  CacheLine *lines_ = nullptr;
  CacheEntry *entries_ = nullptr;
  ibv_mr *mr_ = nullptr;

  FrameHashTable *hash_table_ = nullptr;

  // LRU
  ClockReplacer *replacer_ = nullptr;
  // uint32_t lookup_count_ = 0;
};
}  // namespace kv