#include <cstddef>
#include "kv_engine.h"
#include "rdma_server.h"

using namespace kv;

int main() {
  RemoteEngine *engine = new RemoteEngine();
  engine->start("", "12344");
  while (engine->alive())
    ;
  engine->stop();
  delete  engine;
  return 0;
}
