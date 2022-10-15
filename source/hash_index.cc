#include "hash_index.h"
#include "util/logging.h"
#include "util/memcmp.h"
namespace kv {
SlotMonitor::SlotMonitor() {
  const int per_queue_num = kKeyNum / kPoolShardingNum / kThreadNum;
  for (int i = 0; i < kThreadNum; i++) {
    _free_slots[i] = new Queue(kKeyNum / kPoolShardingNum / kThreadNum);
  }

  for (int t = 0; t < kThreadNum; t++) {
    for (size_t i = 0; i < per_queue_num; i++) {
      _free_slots[t]->enqueue(i + t*per_queue_num);
    }
  }
}

int SlotMonitor::GetNewSlot(KeySlot **out) {
  LOG_ASSERT(out, "empty slot ptr");
  int insert_slot = _free_slots[cur_thread_id]->dequeue();
  KeySlot *slot = &_slots[insert_slot];
  *out = slot;
  return insert_slot;
}

void SlotMonitor::FreeSlot(KeySlot *slot) {
  LOG_ASSERT(slot, "empty slot ptr");
  int slot_idx = static_cast<int>(slot - _slots);
  slot->SetAddr(INVALID_ADDR);
  slot->SetNext(INVALID_ADDR);
  _free_slots[cur_thread_id]->enqueue(slot_idx);
}

HashTable::HashTable(size_t size) {
  int logn = 0;
  while (size >= 2) {
    size /= 2;
    logn++;
  }
  _size = PrimeList[logn];
  _bucket = new int[_size];

  // init bucket
  for (size_t i = 0; i < _size; i++) {
    _bucket[i] = -1;
  }
};

KeySlot *HashTable::Find(const Slice &key, uint32_t hash) {
  uint32_t index = hash % _size;

  int slot_id = _bucket[index];
  KeySlot *slot = nullptr;
  while (slot_id != KeySlot::INVALID_SLOT_ID) {
    slot = _monitor[slot_id];
    if (!memcmp_128bit_eq_a(key.data(), slot->Key())) {
      return slot;
    }
    slot_id = slot->Next();
  }
  return nullptr;
}

// DO NOT check duplicate
KeySlot *HashTable::Insert(const Slice &key, uint32_t hash) {
  KeySlot *new_slot = nullptr;
  int new_slot_id = _monitor.GetNewSlot(&new_slot);
  assert(new_slot);

  uint32_t index = hash % _size;

  int slot_id = _bucket[index];
  if (slot_id == KeySlot::INVALID_SLOT_ID) {
    _bucket[index] = new_slot_id;
    return new_slot;
  }

  KeySlot *slot = nullptr;
  // insert into head
  slot = _monitor[new_slot_id];
  slot->SetNext(_bucket[index]);

  _bucket[index] = new_slot_id;
  return slot;
}

KeySlot *HashTable::Remove(const Slice &key, uint32_t hash) {
  uint32_t index = hash % _size;

  int slot_id = _bucket[index];
  if (slot_id == KeySlot::INVALID_SLOT_ID) {
    return nullptr;
  }

  // head
  KeySlot *slot = _monitor[slot_id];
  if (!memcmp_128bit_eq_a(key.data(), slot->Key())) {
    _bucket[index] = slot->Next();
    return slot;
  }

  // find
  int front_slot_id = slot_id;
  int cur_slot_id = slot->Next();
  while (cur_slot_id != KeySlot::INVALID_SLOT_ID) {
    slot = _monitor[cur_slot_id];
    if (!memcmp_128bit_eq_a(key.data(), slot->Key())) {
      _monitor[front_slot_id]->SetNext(slot->Next());
      return slot;
    }

    front_slot_id = cur_slot_id;
    cur_slot_id = slot->Next();
  }
  // cannot find
  return nullptr;
}
}  // namespace kv