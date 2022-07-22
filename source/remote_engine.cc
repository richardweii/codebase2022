#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include "config.h"
#include "kv_engine.h"
#include "msg.h"
#include "pool.h"
#include "rdma_server.h"
#include "util/filter.h"
#include "util/hash.h"
#include "util/logging.h"

namespace kv {

/**
 * @description: start remote engine service
 * @param {string} addr   empty string for RemoteEngine as server
 * @param {string} port   the port the server listened
 * @return {bool} true for success
 */
bool RemoteEngine::start(const std::string addr, const std::string port) {
  stop_ = false;
  server_ = new RDMAServer(std::bind(&RemoteEngine::handler, this, std::placeholders::_1));
  auto succ = server_->Init(addr, port);
  assert(succ);

  for (int i = 0; i < kPoolShardNum; i++) {
    pool_[i] = new RemotePool(server_->Pd(), i);
  }

  server_->Start();
  return true;
}

/**
 * @description: get engine alive state
 * @return {bool}  true for alive
 */
bool RemoteEngine::alive() {  // TODO
  return server_->Alive();
}

/**
 * @description: stop local engine service
 * @return {void}
 */
void RemoteEngine::stop() { server_->Stop(); }

void RemoteEngine::handler(RPCTask *task) {
  switch (task->RequestType()) {
    case MSG_ALLOC: {
      AllocRequest *req = task->GetRequest<AllocRequest>();
      LOG_DEBUG("Alloc msg, shard %d:", req->shard);
      auto access = pool_[req->shard]->AllocBlock();
      LOG_DEBUG("Alloc successfully, prepare response.");

      AllocResponse resp;
      resp.addr = access.addr;
      resp.rkey = access.rkey;
      resp.status = RES_OK;
      task->SetResponse(resp);
      LOG_DEBUG("Response Alloc msg...");
      break;
    }
    default:
      LOG_ERROR("Invalid message.");
      break;
  }
}

}  // namespace kv
