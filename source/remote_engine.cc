#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include "config.h"
#include "kv_engine.h"
#include "msg.h"
#include "pool.h"
#include "rdma_server.h"
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
  _stop = false;
  _server = new RDMAServer(std::bind(&RemoteEngine::handler, this, std::placeholders::_1));
  auto succ = _server->Init(addr, port);
  assert(succ);

  _pool = new RemotePool(_server->Pd());

  _server->Start();
  return true;
}

/**
 * @description: get engine alive state
 * @return {bool}  true for alive
 */
bool RemoteEngine::alive() {  // TODO
  return _server->Alive();
}

/**
 * @description: stop local engine service
 * @return {void}
 */
void RemoteEngine::stop() { _server->Stop(); }

void RemoteEngine::handler(RPCTask *task) {
  switch (task->RequestType()) {
    case MSG_ALLOC: {
      AllocRequest *req = task->GetRequest<AllocRequest>();
      LOG_INFO("Alloc msg");
      auto access = _pool->AllocBlock();

      AllocResponse resp;
      resp.addr = access.addr;
      resp.rkey = access.rkey;
      resp.status = RES_OK;
      task->SetResponse(resp);
      LOG_INFO("Response Alloc msg...");
      break;
    }
    case MSG_NET_BUFFER : {
      NetBufferInitReq *req = task->GetRequest<NetBufferInitReq>();
      LOG_INFO("NetBuffer Init.");
      _net_buffer_rkey = req->rkey;
      _net_buffer_addr = req->addr;
      _server->_net_buffer_rkey = _net_buffer_rkey;
      _server->_net_buffer_addr = _net_buffer_addr;

      NetBufferInitResponse resp;
      task->SetResponse(resp);
      _server->_start_polling = true;
      break;
    }
    default:
      LOG_ERROR("Invalid message.");
      break;
  }
}

}  // namespace kv
