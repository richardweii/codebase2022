#pragma once
#ifndef _KV_BASE_H
#define _KV_BASE_H


#include <assert.h>
#include <byteswap.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <sys/socket.h>
#include <sys/types.h>

/* Genral definitions */
#define byte unsigned char

#define PRINT_LOG 0

#define RC (0)
#define UC (1)
#define UD (2)
#define RawEth (3)
#define XRC (4)
#define DC (5)
#define SRD (6)

#define OFF (0)
#define ON (1)
#define SUCCESS (0)
#define FAILURE (1)
#define VERSION_EXIT (10)
#define HELP_EXIT (11)
#define MTU_FIX (7)

#define LINK_FAILURE (-1)
#define LINK_UNSPEC (-2)

#define DEF_PORT (18515)
#define DEF_IB_PORT (1)
#define DEF_IB_PORT2 (2)
#define DEF_SIZE_BW (65536)
#define DEF_SIZE_LAT (2)
#define DEF_ITERS (1000)
#define DEF_ITERS_WB (5000)
#define DEF_TX_BW (128)
#define DEF_TX_LAT (1)
#define DEF_CACHE_LINE_SIZE (64)
#define DEF_PAGE_SIZE (4096)

#define NUM_OF_RETRIES (10)

/* Macro for allocating. */
#define ALLOCATE(var, type, size)                                              \
  {                                                                            \
    if ((var = (type *)malloc(sizeof(type) * (size))) == nullptr) {            \
      fprintf(stderr, " Cannot Allocate\n");                                   \
      exit(1);                                                                 \
    }                                                                          \
  }

/* This is our string builder */
#define GET_STRING(orig, temp)                                                 \
  {                                                                            \
    ALLOCATE(orig, char, (strlen(temp) + 1));                                  \
    strcpy(orig, temp);                                                        \
  }

#define CHECK_VALUE(arg, type, name, not_int_ptr)                              \
  {                                                                            \
    arg = (type)strtol(optarg, &not_int_ptr, 0);                               \
    if (*not_int_ptr != '\0') /*not integer part is not empty*/                \
    {                                                                          \
      fprintf(stderr, " %s argument %s should be %s\n", name, optarg, #type);  \
      return 1;                                                                \
    }                                                                          \
  }

/* The type definitions. */
enum MachineType { SERVER, CLIENT, UNCHOSEN };

enum ctx_device {
  DEVICE_ERROR = -1,
  UNKNOWN = 0,
  CONNECTX = 1,
  CONNECTX2 = 2,
  CONNECTX3 = 3,
  CONNECTIB = 4,
  LEGACY = 5,
  CHELSIO_T4 = 6,
  CHELSIO_T5 = 7,
  CONNECTX3_PRO = 8,
  SKYHAWK = 9,
  CONNECTX4 = 10,
  CONNECTX4LX = 11,
  QLOGIC_E4 = 12,
  QLOGIC_AH = 13,
  CHELSIO_T6 = 14,
  CONNECTX5 = 15,
  CONNECTX5EX = 16,
  CONNECTX6 = 17,
  CONNECTX6DX = 18,
  MLX5GENVF = 19,
  BLUEFIELD = 20,
  BLUEFIELD2 = 21,
  INTEL_ALL = 22,
  NETXTREME = 23,
  EFA = 24,
  CONNECTX6LX = 25,
  CONNECTX7 = 26,
  QLOGIC_AHP = 27,
  BLUEFIELD3 = 28,
};

/* The functions. */
enum ctx_device ib_dev_name(ibv_context *context);

ibv_device *ctx_find_dev(char **ib_devname);

const char *link_layer_str(int8_t link_layer);

int check_add_port(char **service, int port, const char *servername,
                   addrinfo *hints, addrinfo **res);

/* Hash implement */
#define RANDOM_1 1.0412321
#define RANDOM_2 1.1131347
#define RANDOM_3 1.0132677

#define HASH_RANDOM_MASK 1463735687
#define HASH_RANDOM_MASK2 1653893711

// TODO: use RW lock
using Sync_Obj = std::mutex;
#define Sync_Obj_destory(latch) (latch->~mutex())
#define Sync_Obj_lock(latch) (latch->lock())
#define Sync_Obj_unlock(latch) (latch->unlock())

#pragma once

#include <condition_variable>
#include <mutex>

struct db_event {
  db_event() {
    mset = false;
    signal_count = 1;
  }
  ~db_event() {}

  std::mutex mtx;
  std::condition_variable cond;
  bool mset;
  int64_t signal_count;

  void set() {
    std::unique_lock<std::mutex> locker(mtx);

    if (!mset) {
      mset = true;
      ++signal_count;
      cond.notify_all();
    }
  }

  int64_t reset() {
    std::unique_lock<std::mutex> locker(mtx);

    if (mset) {
      mset = false;
    }

    int64_t ret = signal_count;
    return ret;
  }

  void wait(int64_t reset_sig_count) {
    std::unique_lock<std::mutex> locker(mtx);

    if (!reset_sig_count) {
      reset_sig_count = signal_count;
    }

    while (!mset && signal_count == reset_sig_count) {
      cond.wait(locker);
    }
  }

protected:
  db_event(const db_event &);
  db_event &operator=(const db_event &);
};


static inline void m_write_1(byte *b, uint64_t n) {
  b[0] = static_cast<byte>(n);
}
static inline void m_write_2(byte *b, uint64_t n) {
  b[0] = static_cast<byte>(n >> 8);
  b[1] = static_cast<byte>(n);
}
static inline void m_write_4(byte *b, uint64_t n) {
  b[0] = static_cast<byte>(n >> 24);
  b[1] = static_cast<byte>(n >> 16);
  b[2] = static_cast<byte>(n >> 8);
  b[3] = static_cast<byte>(n);
}
static inline void m_write_8(byte *b, uint64_t n) {
  m_write_4(b, (uint64_t)(n >> 32));
  m_write_4(b + 4, (uint64_t)(n));
}
static inline uint8_t m_read_1(const byte *b) {
  return (static_cast<uint8_t>(b[0]));
}
static inline uint m_read_2(const byte *b) {
  return ((static_cast<uint>(b[0]) << 8) | (static_cast<uint>(b[1])));
}
static inline uint32_t m_read_4(const byte *b) {
  return ((static_cast<uint32_t>(b[0]) << 24) |
          (static_cast<uint32_t>(b[1]) << 16) |
          (static_cast<uint32_t>(b[2]) << 8) | (static_cast<uint32_t>(b[3])));
}
static inline uint64_t m_read_8(const byte *b) {
  uint64_t u64;
  u64 = m_read_4(b);
  u64 <<= 32;
  u64 |= m_read_4(b + 4);
  return u64;
}


#endif