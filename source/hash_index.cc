#include "hash_index.h"
#include "util/memcmp.h"
namespace kv {
SlotMonitor::SlotMonitor() {
  _free_slot_head = 0;
  for (size_t i = 0; i < (kKeyNum / kPoolShardingNum) - 1; i++) {
    _slots[i].SetNext(i + 1);
  }
}

int SlotMonitor::GetNewSlot(KeySlot **out) {
  LOG_ASSERT(out, "empty slot ptr");

RETRY:
  int old_free_slot_head = _free_slot_head;
  KeySlot *slot = &_slots[old_free_slot_head];
  int next = slot->Next();
  if (_free_slot_head.compare_exchange_strong(old_free_slot_head, next)) {
    slot->SetNext(-1);
    *out = slot;
    return old_free_slot_head;
  }
  goto RETRY;
}

void SlotMonitor::FreeSlot(KeySlot *slot) {
  LOG_ASSERT(slot, "empty slot ptr");
  int old_free_slot_head = _free_slot_head;
  int slot_idx = static_cast<int>(slot - _slots);
  if (_free_slot_head.compare_exchange_strong(old_free_slot_head, slot_idx)) {
    slot->SetAddr(INVALID_ADDR);
    slot->SetNext(old_free_slot_head);
  }
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
  hash >>= kPoolShardingBits;
  uint32_t index = hash % _size;

  // RLock(index);
  // defer { RUnlock(index); };

  int slot_id = _bucket[index];
  KeySlot *slot = nullptr;
  while (slot_id != KeySlot::INVALID_SLOT_ID) {
    slot = _monitor[slot_id];
    if (!memcmp_128bit_eq_a(key.data(), slot->Key())) {
      return slot;
    }
    // if (__int128(*(__int128 *)key.data()) == __int128(*(__int128 *)slot->Key())) {
    //   return slot;
    // }
    // if ((memcmp(key.data(), slot->Key(), kKeyLength) == 0)) {
    //   return slot;
    // }
    slot_id = slot->Next();
  }
  return nullptr;
}

// DO NOT check duplicate
KeySlot *HashTable::Insert(const Slice &key, uint32_t hash) {
  KeySlot *new_slot = nullptr;
  int new_slot_id = _monitor.GetNewSlot(&new_slot);
  assert(new_slot);

  hash >>= kPoolShardingBits;
  uint32_t index = hash % _size;
  // WLock(index);
  // defer { WUnlock(index); };

  int slot_id = _bucket[index];
  if (slot_id == KeySlot::INVALID_SLOT_ID) {
    _bucket[index] = new_slot_id;
    return new_slot;
  }

  KeySlot *slot = nullptr;
#ifdef TEST_CONFIG
  // find
  while (slot_id != KeySlot::INVALID_SLOT_ID) {
    slot = _monitor[slot_id];
    if ((memcmp(key.data(), slot->Key(), kKeyLength) == 0)) {
      LOG_DEBUG("duplicate of slot %d", new_slot_id);
      return nullptr;
    }
    slot_id = slot->Next();
  }
#endif

  // insert into head
  slot = _monitor[new_slot_id];
  slot->SetNext(_bucket[index]);

  _bucket[index] = new_slot_id;
  return slot;
}

KeySlot *HashTable::Remove(const Slice &key, uint32_t hash) {
  hash >>= kPoolShardingBits;
  uint32_t index = hash % _size;

  // WLock(index);
  // defer { WUnlock(index); };

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
  // if (__int128(*(__int128 *)key.data()) == __int128(*(__int128 *)slot->Key())) {
  //   _bucket[index] = slot->Next();
  //   return slot;
  // }
  // if (memcmp(slot->Key(), key.data(), kKeyLength) == 0) {
  //   _bucket[index] = slot->Next();
  //   return slot;
  // }

  // find
  int front_slot_id = slot_id;
  int cur_slot_id = slot->Next();
  while (cur_slot_id != KeySlot::INVALID_SLOT_ID) {
    slot = _monitor[cur_slot_id];
    if (!memcmp_128bit_eq_a(key.data(), slot->Key())) {
      _monitor[front_slot_id]->SetNext(slot->Next());
      return slot;
    }
    // if (__int128(*(__int128 *)key.data()) == __int128(*(__int128 *)slot->Key())) {
    //   _monitor[front_slot_id]->SetNext(slot->Next());
    //   return slot;
    // }
    // if (memcmp(slot->Key(), key.data(), kKeyLength) == 0) {
    //   _monitor[front_slot_id]->SetNext(slot->Next());
    //   return slot;
    // }
    front_slot_id = cur_slot_id;
    cur_slot_id = slot->Next();
  }
  // cannot find
  return nullptr;
}
}  // namespace kv