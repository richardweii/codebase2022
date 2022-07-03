set(BASE_INCLUDE
    kv_engine.h msg.h rdma_conn.h rdma_conn_manager.h rdma_mem_pool.h)

install(FILES   ${BASE_INCLUDE}
    DESTINATION include)