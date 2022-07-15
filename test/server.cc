#include <cstddef>
#include "kv_engine.h"
#include "rdma_server.h"

using namespace kv;

int main() {
    RemoteEngine *engine = new RemoteEngine();
    engine->start("", "12345");
    engine->stop();
}
