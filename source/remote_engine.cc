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
  bloom_filter_ = NewBloomFilterPolicy();
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
void RemoteEngine::stop() {
  server_->Stop();
}

void RemoteEngine::handler(RPCTask *task) {
  switch (task->RequestType()) {
    case MSG_ALLOC: {
      AllocRequest *req = task->GetRequest<AllocRequest>();
      LOG_DEBUG("Alloc msg, shard %d:", req->shard);
      auto access = pool_[req->shard]->AllocDataBlock();
      LOG_DEBUG("Alloc successfully, prepare response.");

      AllocResponse resp;
      resp.addr = access.addr;
      resp.rkey = access.rkey;
      resp.status = RES_OK;
      task->SetResponse(resp, req->sync);
      LOG_DEBUG("Response Alloc msg...");
      break;
    }
    case MSG_LOOKUP: {
      LookupRequest *req = task->GetRequest<LookupRequest>();
      Slice key(req->key, kKeyLength);
      uint32_t hash = Hash(key.data(), kKeyLength, kPoolHashSeed);
      int index = Shard(hash);
      LOG_DEBUG("Lookup msg, key %s, shard %d", key.data(), index);
      BlockId id = pool_[index]->Lookup(key, this->bloom_filter_);

      LookupResponse resp;
      if (id == INVALID_BLOCK_ID) {
        resp.status = RES_FAIL;
        LOG_DEBUG("Failed to find key %s", key.data());
      } else {
        auto access = pool_[index]->AccessDataBlock(id);
        resp.addr = access.addr;
        resp.rkey = access.rkey;
        resp.status = RES_OK;
      }

      task->SetResponse(resp);
      LOG_DEBUG("Response Lookup %s msg, blockid %d", key.data(), id);
      break;
    }
    case MSG_FETCH: {
      FetchRequest *req = task->GetRequest<FetchRequest>();
      LOG_DEBUG("Fetch msg,, shard %d, block %d", req->shard, req->id);
      auto access = pool_[req->shard]->AccessDataBlock(req->id);

      FetchResponse resp;
      resp.addr = access.addr;
      resp.rkey = access.rkey;
      resp.status = RES_OK;

      task->SetResponse(resp);
      LOG_DEBUG("Response Fetch msg, blockid %d", req->id);
      break;
    }
    case MSG_FREE: {
      FreeRequest *req = task->GetRequest<FreeRequest>();
      LOG_DEBUG("Free msg, shard %d block %d:", req->shard, req->id);
      auto ret = pool_[req->shard]->FreeDataBlock(req->id);
      LOG_ASSERT(ret, "Failed to free block %d", req->id);

      FreeResponse resp;
      resp.status = RES_OK;

      task->SetResponse(resp);
      LOG_DEBUG("Response Free msg, blockid %d", req->id);
      break;
    }
    default:
      LOG_ERROR("Invalid message.");
      break;
  }
}

}  // namespace kv
