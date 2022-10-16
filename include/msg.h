#pragma once

#include <assert.h>
#include <stdint.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include "config.h"

#include <cstdint>
namespace kv {

#define MAX_MSG_SIZE 32
#define MAX_REQUEST_SIZE 32
#define MAX_RESPONSE_SIZE 32
#define CHECK_RDMA_MSG_SIZE(T) static_assert(sizeof(T) < MAX_MSG_SIZE - 1, #T " msg size is too big!")

struct PData {
  uint64_t buf_addr;
  uint32_t buf_rkey;
  uint32_t size;
};

enum MsgType { CMD_PING, CMD_STOP, CMD_TEST, MSG_ALLOC, MSG_NET_BUFFER};

enum ResStatus { RES_OK, RES_FAIL };

enum MsgState { IDLE = 0, PREPARED = 0x11, PROCESS = 0x33, ASYNC = 0x55, DONE = 0x77 };

struct RequestBlock {
  uint8_t message[MAX_REQUEST_SIZE - 1];
  std::atomic_uint8_t notify;
};

struct ResponseBlock {
  uint8_t message[MAX_RESPONSE_SIZE - 1];
  std::atomic_uint8_t notify;
};

struct MessageBlock {
  RequestBlock req_block;
  ResponseBlock resp_block;
};

struct RequestsMsg {
  uint32_t rid;
  uint8_t type;
};

struct ResponseMsg {
  uint8_t status;
};

// user defined message
struct PingCmd : public RequestsMsg {
  uint64_t addr;
  uint32_t rkey;
};
CHECK_RDMA_MSG_SIZE(PingCmd);

struct PingResponse : public ResponseMsg {};
CHECK_RDMA_MSG_SIZE(PingResponse);

struct StopCmd : public RequestsMsg {};
CHECK_RDMA_MSG_SIZE(StopCmd);

struct StopResponse : public ResponseMsg {};
CHECK_RDMA_MSG_SIZE(StopResponse);

struct AllocRequest : public RequestsMsg {
  uint64_t size;
  uint8_t shard;
};
CHECK_RDMA_MSG_SIZE(AllocRequest);

struct AllocResponse : public ResponseMsg {
  uint64_t addr;
  uint32_t rkey;
};
CHECK_RDMA_MSG_SIZE(AllocResponse);

struct NetBufferInitReq : public RequestsMsg {
  uint64_t addr;
  uint32_t rkey;
};
CHECK_RDMA_MSG_SIZE(NetBufferInitReq);

struct NetBufferInitResponse : public ResponseMsg {};
CHECK_RDMA_MSG_SIZE(NetBufferInitResponse);

// for test
struct DummyRequest : public RequestsMsg {
  char msg[16];
};
CHECK_RDMA_MSG_SIZE(DummyRequest);

struct DummyResponse : public ResponseMsg {
  char resp[16];
};
CHECK_RDMA_MSG_SIZE(DummyResponse);

}  // namespace kv