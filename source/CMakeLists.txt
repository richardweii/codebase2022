include_directories(${PROJECT_SOURCE_DIR}/include)

set(BASE_SOURCE
    local_engine.cc remote_engine.cc rdma_conn_manager.cc rdma_conn.cc rdma_mem_pool.cc)

add_library(polarkv STATIC ${BASE_SOURCE})

install(TARGETS polarkv
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
        RUNTIME DESTINATION bin
        PUBLIC_HEADER DESTINATION include)
