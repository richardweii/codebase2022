#pragma once

#include <atomic>
#include <cassert>
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "conn_manager.h"
#include "kv_base.h"
#include "kv_hash.h"
#include "kv_mem.h"

struct hash_node_t;
struct hash_key_t;


constexpr uint64_t FIX_BLOCK_LENGTH = (1024 * 1024);

constexpr uint64_t MAX_REMOTE_MEMORY_LENGTH =
    ((uint64_t)(1024 * 30) * (uint64_t)(1024 * 1024));
    
constexpr uint64_t MAX_REMOTE_BLOCK_NUM =
    MAX_REMOTE_MEMORY_LENGTH / FIX_BLOCK_LENGTH;

constexpr uint64_t MAX_LOCAL_MEMORY_LENGTH =
    ((uint64_t)(1024 * 1) * (uint64_t)(1024 * 1024));
constexpr uint64_t MAX_LOCAL_BLOCK_NUM =
    MAX_LOCAL_MEMORY_LENGTH / FIX_BLOCK_LENGTH;

constexpr size_t MAX_BLOCKS_IN_USE = 128;

constexpr size_t LOCAL_FREE_LOW_WM = 64;

enum mr_buf_stat : uint32_t { MR_NOT_INITED = 0, MR_INITED };

struct mr_buf {
  // ibv_mr r_mr; /* buffer to store mr for remote memory */
  uint64_t r_addr;
  uint32_t r_key;
  mr_buf_stat state;
};

struct pinned_block {
  pinned_block(uint64_t voff, uint64_t len)
      : local_mem(nullptr),
        virtual_offset(voff),
        mem_length(len),
        mr(nullptr),
        pinned(false){};
  ~pinned_block(){};

  byte *local_mem;
  std::mutex b_mutex;
  uint64_t virtual_offset;
  uint64_t mem_length;
  ibv_mr *mr;
  bool pinned;

  void lock() { b_mutex.lock(); }
  void unlock() { b_mutex.unlock(); }

  ibv_mr *reg_local_mr(ibv_pd *pd, int flags) {
    if (!pinned && local_mem) {
      assert(mr == nullptr);
      mr = ibv_reg_mr(pd, local_mem, mem_length, flags);
      assert(mr != nullptr);
      pinned = true;
    }
    return mr;
  }
  void unreg_local_mr() {
    if (pinned && mr) {
      ibv_dereg_mr(mr);
      mr = nullptr;
      pinned = false;
    }
  }

  ibv_mr *reg_alloc_local(ibv_pd *pd, int flags, uint64_t alig) {
    assert(local_mem == nullptr);
    assert(mem_length > 0);
    local_mem = (byte *)aligned_alloc(alig, mem_length);
    return reg_local_mr(pd, flags);
  }
  void unreg_free_local() {
    unreg_local_mr();
    if (local_mem) {
      free(local_mem);
      local_mem = nullptr;
    }
  }
};

// TODO: may need memory pool for memory reuse

class Pinned_Fix_Memory {
 public:
  Pinned_Fix_Memory(ibv_pd *p, uint64_t block_len, uint64_t max)
      : next_offset(0),
        max_size(max),
        block_length(block_len),
        align_str(DEF_PAGE_SIZE),
        pd(p) {
    max_blocks = max / block_len;

    blocks.resize(max_blocks);

    mr_buf_size = sizeof(mr_buf) * max_blocks;
    r_mrs = (mr_buf *)malloc(mr_buf_size);
    memset(r_mrs, 0, mr_buf_size);

    r_mrs_mr = ibv_reg_mr(pd, r_mrs, mr_buf_size,
                          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                              IBV_ACCESS_REMOTE_WRITE);
  };
  ~Pinned_Fix_Memory();

  /* variable */
  std::atomic<uint64_t> next_offset; /* also the mem size */
  uint64_t max_size;
  uint64_t max_blocks;
  uint64_t block_length;
  uint64_t align_str;

  /* function */
  // bool allocate_block(uint64_t offset, ibv_pd *pd, int flags);
  bool allocate_next_block(int flags, bool need_lock);
  // bool push_block(uint64_t offset, ibv_pd *pd, int flags);
  // bool push_next_block(ibv_pd *pd, int flags);

  ibv_mr *get_mr(size_t block_num) {
    if (blocks[block_num]) {
      return blocks[block_num]->mr;
    }
    return nullptr;
  }

  byte *get_mem(size_t block_num) {
    if (blocks[block_num]) {
      return blocks[block_num]->local_mem;
    }
    return nullptr;
  }

  pinned_block *get_block(size_t block_num) {
    if (blocks[block_num]) {
      return blocks[block_num];
    }
    return nullptr;
  }

  // mr_buf *get_mr_buf(size_t block_num){
  //   if(blocks[block_num]){
  //     return blocks
  //   }
  // }

  mr_buf *get_r_mrs_buf() { return r_mrs; }
  // uint64_t get_r_mrs_buf_addr() {}
  ibv_mr *get_r_mrs_mr() { return r_mrs_mr; }
  size_t get_mr_buf_size() { return mr_buf_size; }

  void mmlock() { mem_mutex.lock(); }
  void mmunlock() { mem_mutex.unlock(); }

 private:
  std::mutex mem_mutex;  // TODO: lock partition && RWlock
  std::vector<pinned_block *> blocks;
  mr_buf *r_mrs;
  ibv_mr *r_mrs_mr;
  ibv_pd *pd;
  size_t mr_buf_size;
};

// TODO: variable length
class Pinned_Var_Memory {
 public:
  Pinned_Var_Memory() : next_offset(0){};
  ~Pinned_Var_Memory(){};

  /* variable */
  std::atomic<uint64_t> next_offset; /* also the mem size */

  /* function */

 private:
  std::mutex mem_mutex;
  std::unordered_map<uint64_t, std::list<pinned_block *>::iterator> blocks;
  std::list<pinned_block *> virtual_outsets;
};

class MemoryMagr {
 public:
  MemoryMagr(ibv_pd *pd)
      : p_fix_memory(pd, FIX_BLOCK_LENGTH, MAX_REMOTE_MEMORY_LENGTH){};
  MemoryMagr(ibv_pd *pd, uint32_t b_num)
      : p_fix_memory(pd, FIX_BLOCK_LENGTH, FIX_BLOCK_LENGTH * b_num){};
  ~MemoryMagr(){};

  /* allocate and make mr for a memory block */
  bool allocate_pinned_memory(int flags, uint64_t len = FIX_BLOCK_LENGTH,
                              bool fixed = true);
  bool allocate_all_fix(int flags);
  // TODO: /* make mr for an allocated memory block */

  /* get mr for pinned memory block */
  ibv_mr *get_pinned_mr(uint64_t voff, uint64_t len, bool fixed = true);

  ibv_mr *get_pinned_mr(uint32_t b_num, bool fixed = true);
  byte *get_pinned_mem(uint32_t b_num, bool fixed = true);
  pinned_block *get_pinned_block(uint32_t b_num);

  int prepare_all(int flags);

  Pinned_Fix_Memory *const get_fix_mem() { return &p_fix_memory; }

 protected:
  /* data */
  Pinned_Fix_Memory p_fix_memory;
  // Pinned_Var_Memory p_var_memory;
};

/*********** on local *************/
class FixedRemoteMemory {
 public:
  FixedRemoteMemory(rdma::ConnectionManager *c, size_t len = FIX_BLOCK_LENGTH,
                    size_t max = MAX_REMOTE_BLOCK_NUM);
  ~FixedRemoteMemory();

  /* allocate and make mr for a memory block */
  int require_remote_memory(bool wait);

  bool is_prepared() { return prepared.load(); }
  void set_prepared(bool p) { prepared.store(p); }

  mr_buf *get_remote_mr(uint64_t block) {
    if (r_mrs[block].state == MR_INITED) {
      return &r_mrs[block];
    }
    return nullptr;
  }
  size_t block_len() { return block_length; }
  size_t max_block_num() { return max_blocks; }
  size_t memory_size() { return block_length * max_blocks; }

  void close_remote();
  inline ibv_pd *get_pd() { return conn->get_pd(); }

  // int rdma_write(rdma_mem_info &info) { return conn->rdma_write(info); };
  // int rdma_read(rdma_mem_info &info) { return conn->rdma_read(info); }

 protected:
  rdma::ConnectionManager *conn;
  mr_buf *r_mrs;
  ibv_mr *r_mrs_mr;
  size_t block_length;
  size_t max_blocks;
  std::atomic_bool prepared;
};

enum mem_block_state : uint16_t {
  BLOCK_NOT_USED,       // in free_list
  BLOCK_READY_FOR_USE,  // in pinned_list
  BLOCK_ACTIVE,         // in pinned_list
  BLOCK_FULL            // in pinned_list
};

/* meta data storage for mem */
struct mem_block_t {
  /* If this block is stored on locate */
  byte *data;
  /* If this block is stored on remote */
  // ibv_mr *mr;
  mr_buf *mr_buf_;

  uint64_t virtual_offset;
  uint64_t mem_length;
  uint64_t block_num;

  mem_block_state state;

  std::list<mem_block_t *>::iterator itr;

  Sync_Obj *latch;

  /* append record space in block, NOTE: must hold block latch */
  byte *reserve_append_space(uint64_t len) {
    byte *off = data;

    if (cursor + len <= mem_length) {
      off += cursor;
      cursor += len;
      return off;
    } else {
      return nullptr;
    }
  }

  uint64_t get_cursor() { return cursor; }

 private:
  uint64_t cursor{0};
};

/* memory pool */
// TODO: LRU for local and remote block migration
class MemoryPool {
 public:
  MemoryPool(FixedRemoteMemory *rm, rdma::ConnectionManager *conn,
             uint64_t lsize, uint64_t len);
  ~MemoryPool();

  void start_background_evictor();
  void stop_background_evictor();
  void background_evictor();
  void lr_switch_block(mem_block_t *l, mem_block_t *r);
  /* exchange the local free block and remote pinned block */
  void switch_blocks_and_process_list(mem_block_t *l, mem_block_t *r);

  /* get a free memory block to use */
  mem_block_t *get_free_local_block();
  mem_block_t *get_free_local_block_only();
  mem_block_t *get_free_remote_block_only();
  mem_block_t *get_local_replace_block_only();

  /* get a pinned memory block to use */
  mem_block_t *get_pinned_local_block();

  /* free memory block for reuse*/
  void free_block(mem_block_t *block);

  mem_block_t *get_block(uint32_t blockid) {
    assert(blockid < total_block_num);
    return &(blocks[blockid]);
  }

  // bool reserve_record(std::string &v, uint32_t block_id, uint64_t
  // value_length,
  //                     uint64_t offset_in_block);

  bool get_record(std::string &v, uint32_t block_id, uint64_t value_length,
                  uint64_t offset_in_block);

  bool update_record(const std::string &v, uint32_t blcok_id,
                     uint64_t value_length, uint64_t offset_in_block,
                     hash_key_t key);

  hash_node_t *reserve_record(hash_node_t *node, const std::string &v,
                              uint32_t block_id, uint64_t value_length,
                              uint64_t offset_in_block, hash_key_t key);

  mem_block_t *get_locked_block_in_use(uint32_t tid);

  void remote_write(uint64_t src, uint64_t dest, uint32_t len,
                           uint32_t rkey);
  void remote_read(uint64_t src, uint64_t dest, uint32_t len,
                          uint32_t rkey);


 private:
  byte *local_data; /* storage for local data */
  FixedRemoteMemory *rmem;
  rdma::ConnectionManager *conn;

  std::atomic<uint64_t> m_count_{0};

  /* bind with thread not key shard */
  /* MAX_BLOCKS_IN_USE partition */
  MemoryMagr ltmp_mem;
  mem_block_t *block_in_use[MAX_BLOCKS_IN_USE];
  Sync_Obj *users_latch[MAX_BLOCKS_IN_USE];

  uint64_t remote_size;
  uint64_t local_size;
  uint64_t total_size;

  uint64_t block_length;

  uint32_t remote_block_num;
  uint32_t local_block_num;
  uint32_t total_block_num;

  mem_block_t *blocks; /* array of cbs_buf_block_t */
  Sync_Obj *blocks_latch;

  std::list<mem_block_t *> local_free_list;
  std::list<mem_block_t *> local_pinned_list;

  std::list<mem_block_t *> remote_free_list;
  std::list<mem_block_t *> remote_pinned_list;

  std::list<mem_block_t *> local_full_list;

  std::list<mem_block_t *> used_block_list;

  Sync_Obj *lfree_list_latch;
  Sync_Obj *rfree_list_latch;
  Sync_Obj *lpinned_list_latch;
  Sync_Obj *rpinned_list_latch;
  Sync_Obj *lfull_list_latch;
  Sync_Obj *used_list_latch;

  db_event free_event;
  db_event evict_event;

  bool doing_evict;
  bool stop_evict;

  void block_init(mem_block_t *b, byte *data, size_t n);
};
