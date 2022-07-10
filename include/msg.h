#pragma once

#include <assert.h>
#include <stdint.h>
#include <chrono>
#include <cstdint>
#include "config.h"

namespace kv {

#define NOTIFY_WORK 0xFF
#define NOTIFY_IDLE 0x00
#define MAX_MSG_SIZE 32
#define MAX_SERVER_WORKER 4
#define RESOLVE_TIMEOUT_MS 5000
#define RDMA_TIMEOUT_US 10000000  // 10s
#define MAX_REMOTE_SIZE (1UL << 25)

#define TIME_NOW (std::chrono::high_resolution_clock::now())
#define TIME_DURATION_US(START, END) (std::chrono::duration_cast<std::chrono::microseconds>((END) - (START)).count())

enum MsgType { MSG_PING, MSG_ALLOC, MSG_LOOKUP, MSG_FREE, MSG_STOP };

enum ResStatus { RES_OK, RES_FAIL };

#define CHECK_RDMA_MSG_SIZE(T) static_assert(sizeof(T) < MAX_MSG_SIZE, #T " msg size is too big!")

struct PData {
  uint64_t buf_addr;
  uint32_t buf_rkey;
  uint32_t size;
};

struct CmdMsgBlock {
  uint8_t rsvd1[MAX_MSG_SIZE - 1];
  volatile uint8_t notify;
};

struct CmdMsgRespBlock {
  uint8_t rsvd1[MAX_MSG_SIZE - 1];
  volatile uint8_t notify;
};

struct RequestsMsg {
  uint8_t type;
};
CHECK_RDMA_MSG_SIZE(RequestsMsg);

struct ResponseMsg {
  uint8_t status;
};
CHECK_RDMA_MSG_SIZE(ResponseMsg);

struct PingMsg : RequestsMsg {
  uint64_t resp_addr;
  uint32_t resp_rkey;
};
CHECK_RDMA_MSG_SIZE(PingMsg);

struct StopMsg : RequestsMsg {};
CHECK_RDMA_MSG_SIZE(StopMsg);

struct AllocRequest : public RequestsMsg {
  uint8_t shard;
};
CHECK_RDMA_MSG_SIZE(AllocRequest);

struct AllocResponse : public ResponseMsg {
  uint64_t addr;
  uint32_t rkey;
};
CHECK_RDMA_MSG_SIZE(AllocResponse);

struct LookupRequest : public RequestsMsg {
  char key[kKeyLength];
};
CHECK_RDMA_MSG_SIZE(LookupRequest);

struct LookupResponse : public ResponseMsg {
  uint64_t addr;
  uint32_t rkey;
};
CHECK_RDMA_MSG_SIZE(LookupResponse);

struct FreeRequest : public RequestsMsg {
  uint8_t shard;
  BlockId id;
};
CHECK_RDMA_MSG_SIZE(FreeRequest);

struct FreeResponse : public ResponseMsg {};
CHECK_RDMA_MSG_SIZE(FreeResponse);

}  // namespace kv