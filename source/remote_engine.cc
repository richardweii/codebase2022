#include <cassert>
#include <cstdint>
#include <functional>
#include "kv_engine.h"
#include "logging.h"
#include "msg.h"
#include "rdma_server.h"

#define MEM_ALIGN_SIZE 4096

namespace kv {

/**
 * @description: start remote engine service
 * @param {string} addr   empty string for RemoteEngine as server
 * @param {string} port   the port the server listened
 * @return {bool} true for success
 */
bool RemoteEngine::start(const std::string addr, const std::string port) {
  m_stop_ = false;
  server_ = new RDMAServer(std::bind(&RemoteEngine::handler, this, std::placeholders::_1));
  auto succ = server_->Init(addr, port);
  assert(succ);
  server_->Start();
  return true;
}

/**
 * @description: get engine alive state
 * @return {bool}  true for alive
 */
bool RemoteEngine::alive() {  // TODO
  return true;
}

/**
 * @description: stop local engine service
 * @return {void}
 */
void RemoteEngine::stop() {
  m_stop_ = true;
  // TODO: release resources
}

struct ibv_mr *RemoteEngine::rdma_register_memory(void *ptr, uint64_t size) {
  struct ibv_mr *mr =
      ibv_reg_mr(server_->Pd(), ptr, size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
  if (!mr) {
    perror("ibv_reg_mr fail");
    return nullptr;
  }
  return mr;
}

int RemoteEngine::allocate_and_register_memory(uint64_t &addr, uint32_t &rkey, uint64_t size) {
  /* align mem */
  uint64_t total_size = size + MEM_ALIGN_SIZE;
  uint64_t mem = (uint64_t)malloc(total_size);
  addr = mem;
  if (addr % MEM_ALIGN_SIZE != 0) addr = addr + (MEM_ALIGN_SIZE - addr % MEM_ALIGN_SIZE);
  struct ibv_mr *mr = rdma_register_memory((void *)addr, size);
  if (!mr) {
    perror("ibv_reg_mr fail");
    return -1;
  }
  rkey = mr->rkey;
  // printf("allocate and register memory %ld %d\n", addr, rkey);
  // TODO: save this memory info for later delete
  return 0;
}

void RemoteEngine::handler(RPCTask *task) {
  switch (task->RequestType()) {
    case MSG_ALLOC: {
      LOG_INFO("Alloc msg");
      AllocRequest *req = task->GetRequest<AllocRequest>();
      uint64_t addr;
      uint32_t rkey;
      auto ret = allocate_and_register_memory(addr, rkey, req->size);
      LOG_ASSERT(ret == 0, "failed");
      AllocResponse resp;
      resp.addr = addr;
      resp.reky = rkey;
      resp.status = RES_OK;
      task->SetResponse(resp, true);
    }
    case MSG_FREE:
    case MSG_LOOKUP:
    case MSG_FETCH:
      break;
    default:
      LOG_ERROR("Invalid RPC.");
  }
}

}  // namespace kv
