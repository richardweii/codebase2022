#include <sys/socket.h>
#include <cstring>
#include <thread>
#include "logging.h"
#include "msg.h"
#include "rdma_client.h"

using namespace kv;

int main() {
    RDMAClient *client = new RDMAClient();
    client->Init("172.16.5.129", "12345");
    client->Start();
    DummyRequest req;
    memset(req.msg, 0, 16);
    DummyResponse resp;
    memcpy(req.msg, "hello world", 11);
    req.type = MSG_ALLOC;
    int ret = client->RPC(&req, resp);
    LOG_ASSERT(ret == 0, "Rpc failed");
    LOG_ASSERT(resp.status == RES_OK, "invalid response");
    LOG_INFO("recv %s", resp.resp);
    client->Stop();
}