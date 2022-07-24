#pragma once

#include <infiniband/verbs.h>
#include <sys/types.h>
#include <atomic>
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
  kv::Addr Addr;
  bool Dirty = false;
  CacheLine *Data() const { return line_; }
  bool Valid() const { return valid_; }

 private:
  CacheEntry() = default;
  CacheLine *line_ = nullptr;
  int fid_ = INVALID_FRAME_ID;
  bool valid_ = false;
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

  // thread-safe
  void Pin(FrameId frame_id) { frames_[frame_id].ref_pin_.fetch_add(1); }

  // thread-safe
  void Unpin(FrameId frame_id) {
    frames_[frame_id].ref_pin_.fetch_or(REF);
    frames_[frame_id].ref_pin_.fetch_add(-1);
  }

 private:
  struct Frame {
    Frame() = default;
    FrameId frame_ = INVALID_FRAME_ID;
    std::atomic_uint8_t ref_pin_{0};
  };

  int walk() {
    int ret = hand_;
    hand_ = (hand_ + 1) % frame_num_;
    return ret;
  }

  FrameId pop() {
    while (true) {
      uint8_t tmp = 0;
      if (frames_[hand_].ref_pin_.load() == 0) {
        break;
      } else {
        frames_[hand_].ref_pin_.fetch_and(UNREF);
        walk();
      }
    }
    return walk();
  }
  int hand_ = 0;
  size_t frame_num_;
  Frame *frames_ = nullptr;
  std::list<FrameId> free_list_;
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
    handler_ = new CacheHashTableHanler();
    replacer_ = new ClockReplacer(cache_line_num_);
    hash_table_ = new HashTable<uint32_t>(cache_line_num_, handler_);
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
    delete[] lines_;
    delete handler_;
    delete hash_table_;
    delete replacer_;
  }

  // return a cacheHandle used to write new data to it, if old data is dirty, need to write to remote first
  CacheEntry *Insert(Addr addr);

  void Release(CacheEntry *entry);

  CacheEntry *Lookup(Addr addr);

  ibv_mr *MR() const { return mr_; }

 private:
  class CacheHashTableHanler : public HashHandler<uint32_t> {
   public:
    uint32_t GetKey(uint64_t data_handle) override { return data_handle >> 32; }
    uint32_t GetFrameId(uint64_t data_handle) { return data_handle & 0xffffffff; }
    uint64_t GenHandle(uint32_t key_index, FrameId fid) { return (uint64_t)key_index << 32 | fid; }
  };

  size_t cache_line_num_;
  CacheLine *lines_ = nullptr;
  CacheEntry *entries_ = nullptr;
  ibv_mr *mr_ = nullptr;

  CacheHashTableHanler *handler_ = nullptr;
  HashTable<uint32_t> *hash_table_ = nullptr;

  // LRU
  ClockReplacer *replacer_ = nullptr;
};
}  // namespace kv