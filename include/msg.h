#pragma once

#include <cstdint>
namespace kv {

#define MAX_MSG_SIZE 64
#define MAX_REQUEST_SIZE 32
#define MAX_RESPONSE_SIZE 32
#define CHECK_RDMA_MSG_SIZE(T) static_assert(sizeof(T) < MAX_MSG_SIZE, #T " msg size is too big!")

struct PData {
  uint64_t buf_addr;
  uint32_t buf_rkey;
  uint32_t size;
};

enum MsgType { MSG_ALLOC, MSG_FREE, MSG_PING, MSG_STOP, MSG_LOOKUP, MSG_FETCH };

enum ResStatus { RES_OK, RES_FAIL };

enum MsgState { FREE = 0, PREPARED = 0x11, PROCESS = 0x33, DONE = 0x77 };

struct RequestBlock {
  uint8_t message[MAX_REQUEST_SIZE - 1];
  volatile uint8_t notify;
};

struct ResponseBlock {
  uint8_t message[MAX_RESPONSE_SIZE - 1];
  volatile uint8_t notify;
};

struct MessageBlock {
  RequestBlock req_block;
  ResponseBlock resp_block;
};

struct RequestsMsg {
  uint8_t type;
};

struct ResponseMsg {
  uint8_t status;
};

// user defined message
struct PingRequest : public RequestsMsg {
  uint64_t addr;
  uint32_t rkey;
};
CHECK_RDMA_MSG_SIZE(PingRequest);

struct PingResponse : public ResponseMsg {};
CHECK_RDMA_MSG_SIZE(PingResponse);

// used for test
struct DummyRequest : public RequestsMsg {
  char msg[16];
};
CHECK_RDMA_MSG_SIZE(DummyRequest);

struct DummyResponse : public ResponseMsg {
  char resp[16];
};
CHECK_RDMA_MSG_SIZE(DummyResponse);

}  // namespace kv