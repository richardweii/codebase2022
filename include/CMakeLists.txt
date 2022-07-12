set(BASE_INCLUDE
    kv_engine.h msg.h rdma_conn.h rdma_conn_manager.h rdma_mem_pool.h)

include_directories(${PROCJECT_SOURCE_DIR}/include)

install(FILES   ${BASE_INCLUDE}
    DESTINATION include)