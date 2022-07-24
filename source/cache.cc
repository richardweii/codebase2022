#include "cache.h"
#include <cassert>
#include <cstddef>
#include "config.h"
#include "hash_table.h"
#include "util/logging.h"

namespace kv {

LRUReplacer::LRUReplacer(size_t frame_num) : frame_num_(frame_num) {
  frames_ = new Frame[frame_num];
  for (size_t i = 0; i < frame_num; i++) {
    frames_[i].frame_ = i;
    free_list_.push_back(i);
  }
}

LRUReplacer::~LRUReplacer() { delete[] frames_; }

bool LRUReplacer::Victim(FrameId *frame_id) {
  assert(frame_id != nullptr);
  *frame_id = this->pop();
  return *frame_id != INVALID_FRAME_ID;
}

void LRUReplacer::Pin(FrameId frame_id) {
  if (frames_[frame_id].pin == 0) {
    this->deleteFrame(frame_id);
  }
  frames_[frame_id].pin++;
}

void LRUReplacer::Unpin(FrameId frame_id) {
  frames_[frame_id].pin--;
  if (frames_[frame_id].pin == 0) {
    this->insertBack(frame_id);
  }
}

void LRUReplacer::insertBack(FrameId frame_id) {
  Frame *frame = &frames_[frame_id];
  // add to frame_list
  if (frame_list_head_ == nullptr) {
    assert(frame_list_head_ == frame_list_tail_);
    frame_list_head_ = frame;
    frame_list_tail_ = frame;
  } else {
    frame_list_tail_->next = frame;
    frame->front = frame_list_tail_;
    frame_list_tail_ = frame;
  }
}

void LRUReplacer::deleteFrame(FrameId frame_id) {
  Frame *frame = &frames_[frame_id];
  // remove from list
  if (frame->front == nullptr && frame->next == nullptr) {
    frame_list_head_ = nullptr;
    frame_list_tail_ = nullptr;
  } else if (frame->front == nullptr) {  // head
    frame_list_head_ = frame->next;
    frame_list_head_->front = nullptr;
  } else if (frame->next == nullptr) {  // tail
    frame_list_tail_ = frame->front;
    frame_list_tail_->next = nullptr;
  } else {
    frame->front->next = frame->next;
    frame->next->front = frame->front;
  }
  frame->next = nullptr;
  frame->front = nullptr;
}

FrameId LRUReplacer::pop() {
  if (!free_list_.empty()) {
    FrameId fid = free_list_.front();
    free_list_.pop_front();
    return fid;
  }

  if (frame_list_head_ == nullptr) {
    return INVALID_FRAME_ID;
  }

  // remove from list
  auto frame = frame_list_head_;
  frame_list_head_ = frame_list_head_->next;
  if (frame_list_head_ != nullptr) {
    frame_list_head_->front = nullptr;
  } else {
    frame_list_tail_ = nullptr;
  }
  FrameId ret = frame->frame_;
  return ret;
}

CacheEntry *Cache::Insert(Addr addr) {
  addr.RoundUp();
  FrameId fid;
  auto succ = replacer_->Victim(&fid);
  assert(succ);

  CacheEntry *victim = &entries_[fid];

  if (victim->Valid()) {
    // remove from old hash table
    hash_table_->Remove(victim->Addr.RawAddr());
  }
  // add to hash table
  hash_table_->Insert(addr.RawAddr(), handler_->GenHandle(addr.RawAddr(), fid));

  // if (victim->Valid()) {
  replacer_->Pin(fid);
  // }
  victim->valid_ = true;
  return victim;
}

void Cache::Release(CacheEntry *entry) { replacer_->Unpin(entry->fid_); }

CacheEntry *Cache::Lookup(Addr addr) {
  addr.RoundUp();
  auto node = hash_table_->Find(addr.RawAddr());
  if (node == nullptr) {
    return nullptr;
  }

  FrameId fid = handler_->GetFrameId(node->Handle());
  LOG_ASSERT(addr == entries_[fid].Addr, "Unmatched key. expect %u, got %u", addr.RawAddr(),
             entries_[fid].Addr.RawAddr());
  replacer_->Pin(fid);
  return &entries_[fid];
}

}  // namespace kv