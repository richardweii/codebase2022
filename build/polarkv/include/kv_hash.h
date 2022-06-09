#pragma once

#include "kv_mem.h"
#include <string>
#include <assert.h>

/**** Physical record ****/
constexpr size_t KEY_BYTE_LEN = 16;
constexpr size_t VALUE_BYTE_LEN = 128;
constexpr size_t REC_START_LEN = 10;
constexpr size_t MAX_KEY_NUM = 200 * 1000 * 1000;
static constexpr uint64_t HASH_CELL_NUM = 1024 * 1024 * 64;

class Data {
public:
  Data(const char *d, size_t n) : data(d), size(n) {}
  Data(const std::string &s) : data(s.data()), size(s.size()) {}
  Data(const char *s) : data(s) { size = (s == nullptr) ? 0 : strlen(s); }

  size_t get_size() const { return size; }
  const char *get_data() const { return data; }

  std::string ToString(bool hex = false) const;

private:
  const char *data;
  size_t size;

  static inline char toHex(unsigned char v) {
    if (v <= 9)
      return '0' + v;
    return 'A' + v - 10;
  }
};

/**** Hash index ****/
struct hash_key_t {
  // TODO: variable length key
  // TODO: no copy interface
  hash_key_t(const char *data, bool copy = true) {
    assert(copy);
    if (copy) {
      memcpy(fixkey, (byte *)(data), KEY_BYTE_LEN);
    }
  }

  hash_key_t(const Data &data, bool copy = true) {
    assert(copy);
    if (copy) {
      memcpy(fixkey, (byte *)const_cast<char *>(data.get_data()), KEY_BYTE_LEN);
    }
  }

  hash_key_t(const hash_key_t &data, bool copy = true) {
    assert(copy);
    if (copy) {
      memcpy(fixkey, data.fixkey, KEY_BYTE_LEN);
    }
  }

  byte fixkey[KEY_BYTE_LEN];

  bool operator==(const hash_key_t &rhs) {
    int ret = memcmp(fixkey, rhs.fixkey, KEY_BYTE_LEN);
    if (ret == 0)
      return true;
    return false;
  }

  bool operator!=(const hash_key_t &rhs) {
    assert(false);
    int ret = memcmp(fixkey, rhs.fixkey, KEY_BYTE_LEN);
    if (ret == 0)
      return false;
    return true;
  }

  void operator=(const hash_key_t &rhs) {
    memcpy(fixkey, rhs.fixkey, KEY_BYTE_LEN);
  }

  std::string ToString(bool hex) const {
    std::string result; // RVO/NRVO/move
    if (hex) {
      result.reserve(2 * KEY_BYTE_LEN);
      for (size_t i = 0; i < KEY_BYTE_LEN; ++i) {
        unsigned char c = fixkey[i];
        result.push_back(toHex(c >> 4));
        result.push_back(toHex(c & 0xf));
      }
      return result;
    } else {
      result.assign((char *)fixkey, KEY_BYTE_LEN);
      return result;
    }
  }

  static inline char toHex(unsigned char v) {
    if (v <= 9)
      return '0' + v;
    return 'A' + v - 10;
  }
};

struct hash_node_t {
  hash_node_t(const char *data) : next(nullptr), key(data) {}
  hash_node_t(const Data &data) : next(nullptr), key(data) {}
  hash_node_t(const hash_key_t &data) : next(nullptr), key(data) {}

  hash_key_t key;
  hash_node_t *next;
  uint32_t block_id;
  uint32_t offset_in_block;
  uint8_t value_length;
};

struct hash_cell_t {
  hash_node_t *node;
};

class HashIndex {
public:
  HashIndex(uint64_t n);
  ~HashIndex();

  void global_lock() { Sync_Obj_lock(global_latch); }
  void global_unlock() { Sync_Obj_unlock(global_latch); }

  uint64_t get_cell_num() { return n_cells; }
  
  hash_node_t* fetch_node(){
    if(node_count_.load() >= MAX_KEY_NUM){
      assert(false);
    }
    return hash_node_list_ + node_count_.fetch_add(1);
  }
  uint64_t get_fold(const hash_key_t &key);
  uint64_t get_hash(const hash_key_t &key);

  hash_node_t *hash_search(const hash_key_t &key);
  hash_node_t *hash_insert(const hash_key_t &key, hash_node_t *node);
  hash_node_t *hash_delete(const hash_key_t &key);
  hash_node_t *hash_update(const hash_key_t &key, hash_node_t *node);

private:
  uint64_t n_cells;   /* number of cells in the hash table */
  hash_cell_t *cells; /*!< pointer to cell array */

  Sync_Obj *global_latch;
  std::atomic<uint64_t> node_count_{0};
  hash_node_t* hash_node_list_;

  uint64_t get_prime(uint64_t n);
  uint64_t fold_two(uint64_t n1, uint64_t n2);

  hash_cell_t *get_cell(const hash_key_t &key);
  hash_node_t *get_next_node(hash_node_t *node) { return node->next; }


public:
  static constexpr size_t SHARDS_COUNT = 1024 * 16;

  size_t get_shard(const hash_key_t &key);
  Sync_Obj *get_latch(const hash_key_t &key) {
    return shard_latches[get_shard(key)];
  }
  Sync_Obj *get_latch(size_t shard) { return shard_latches[shard]; }

private:
  class ShardLatches {
    /* Each shard is protected by a separate latch. */
    Sync_Obj *latches[SHARDS_COUNT];

  public:
    ShardLatches() {
      for (size_t i = 0; i < SHARDS_COUNT; ++i) {
        latches[i] = static_cast<Sync_Obj *>(malloc(sizeof(Sync_Obj)));
        new (latches[i]) Sync_Obj;
      }
    };
    ~ShardLatches() {
      fprintf(stdout, "Free %d ShardLatches.\n", SHARDS_COUNT);
      for (size_t i = 0; i < SHARDS_COUNT; ++i) {
        Sync_Obj_destory(latches[i]);
        free(latches[i]);
        latches[i] = nullptr;
      }
    };
    Sync_Obj *operator[](size_t id) { return latches[id]; }
  };

  ShardLatches shard_latches;
};