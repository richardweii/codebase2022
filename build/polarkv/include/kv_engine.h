#pragma once
#include <atomic>
#include <string>
#include "kv_mem.h"
#include "msg.h"
#include "kv_base.h"
#define MAX_VALUE_SIZE 4096

namespace kv
{

  /* Abstract base engine */
  class Engine
  {
  public:
    virtual ~Engine();

    virtual bool start(const std::string addr, const std::string port) = 0;
    virtual void stop() = 0;

    virtual bool alive() = 0;
  };

  /* Local-side engine */
  class LocalEngine : public Engine
  {
  public:
    virtual ~LocalEngine();

    bool start(const std::string addr, const std::string port) override;
    void stop() override;
    bool alive() override {
      return true;
    };

    bool write(const std::string key, const std::string value);
    bool read(const std::string key, std::string &value);

  private:
    rdma::ConnectionManager *m_rdma_conn_;
    /* External Part; */
    HashIndex *m_hash_index_;    /* store the hash index of <key, metadata>;      */
    MemoryPool *m_mem_pool_;     /* memory pool used to manage the memory blocks; */
    FixedRemoteMemory *m_r_mem_; /* struct to store the meta remote memory infos; */

    enum kvstat : uint8_t
    {
      stopped = 0,
      stopping,
      started,
      starting
    };
    std::atomic<uint8_t> m_state_; /**/
  };

  /* Remote-side engine */
  class RemoteEngine : public Engine
  {
  public:
    virtual ~RemoteEngine();

    bool start(const std::string addr, const std::string port) override;
    void stop() override;
    bool alive() override {
      return m_alive_.load();
    }

    bool is_alive() { return m_alive_.load(); }
    void set_alive(bool alive) { m_alive_.store(alive); }
    int join();
  private:
    void handle_connection();

    int create_connection(struct rdma_cm_id *cm_id);

    struct ibv_mr *rdma_register_memory(void *ptr, uint32_t size);

    int remote_write(WorkerInfo *work_info, uint64_t local_addr, uint32_t lkey,
                     uint32_t length, uint64_t remote_addr, uint32_t rkey);

    int allocate_and_register_memory(uint64_t &addr, uint32_t &rkey,
                                     uint32_t size);

    void worker(WorkerInfo *work_info, uint32_t num);

    struct rdma_event_channel *m_cm_channel_;
    struct rdma_cm_id *m_listen_id_;
    struct ibv_pd *m_pd_;
    struct ibv_context *m_context_;
    bool m_stop_;
    std::thread *m_conn_handler_;
    WorkerInfo **m_worker_info_;
    uint32_t m_worker_num_;
    std::thread **m_worker_threads_;
    std::atomic<uint8_t> m_conn_num_; // num of conncections
    std::atomic<bool> m_alive_{true};
  };

}