#include "msg_buf.h"
#include "logging.h"
#include "rdma_client.h"

using namespace kv;

int main() {
  MsgBuffer buf(nullptr);
  auto a1 = buf.AllocMessage();
  LOG_INFO("index %d", buf.MessageIndex(a1));
  auto a2 = buf.AllocMessage();
  LOG_INFO("index %d", buf.MessageIndex(a2));
  auto a3 = buf.AllocMessage();
  LOG_INFO("index %d", buf.MessageIndex(a3));
  for (int i = 0; i < 13; i++) {
    auto a4 = buf.AllocMessage();
    LOG_INFO("index %d", buf.MessageIndex(a4));
  }
  buf.FreeMessage(a1);
  buf.FreeMessage(a3);
  a3 = buf.AllocMessage();
  LOG_INFO("index %d", buf.MessageIndex(a3));
  LOG_INFO("index %d", buf.MessageIndex(buf.AllocMessage()));
  return 0;
}