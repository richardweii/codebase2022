#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <string>
#include <thread>
#include <vector>
#include "config.h"
#include "util/logging.h"
#include "msg.h"
#include "rdma_client.h"

using namespace kv;

constexpr int thread_num = 8;
constexpr int op_per_thread_num = 1 << 15;

std::atomic_uint64_t rid_counter{};

int main(int argc, const char **argv) {
  bool sync = false;
  if (argc > 1) {
    sync = std::atoi(argv[1]);
    LOG_INFO("Sync %d", sync);
  }
  RDMAClient *client = new RDMAClient();
  client->Init("192.168.200.22", "12345");
  client->Start();

  std::vector<std::thread> threads;

  auto time_now = TIME_NOW;

  for (int i = 0; i < thread_num; i++) {
    threads.emplace_back([&]() {
      for (int j = 0; j < op_per_thread_num; j++) {
        DummyRequest req;
        req.sync = sync;
        req.rid = rid_counter.fetch_add(1);
        memset(req.msg, 0, 16);
        auto a = std::to_string(j);
        memcpy(req.msg, a.c_str(), a.size());
        req.type = CMD_TEST;

        DummyResponse resp;
        int ret = client->RPC(req, resp, req.sync);

        std::string res = resp.resp;

        LOG_ASSERT(a == res, "rid %d, expected %s, got %s", req.rid, a.c_str(), res.c_str());
      }
      LOG_INFO("Finish!");
    });
  }

  for (auto &th : threads) {
    th.join();
  }

  auto time_end = TIME_NOW;
  auto time_delta = time_end - time_now;
  auto count = std::chrono::duration_cast<std::chrono::microseconds>(time_delta).count();

  std::cout << "Latency : " << 1.0 * count / op_per_thread_num  / thread_num << "us" << std::endl;
  std::cout << "Ops : " << 1000.0 * op_per_thread_num * thread_num / count << "kps" << std::endl;

  client->Stop();
}