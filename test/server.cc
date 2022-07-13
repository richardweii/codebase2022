#include "rdma_server.h"

using namespace kv;

int main() {
    RDMAServer *server = new RDMAServer();
    server->Init("", "12345");
    server->Start();
}
