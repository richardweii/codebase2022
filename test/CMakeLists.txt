include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/test)

add_executable(
    rdma_client
    simple_test/client.cc
)
target_link_libraries(rdma_client polarkv rdmacm ibverbs ibumad pci)

add_executable(
    rdma_server
    simple_test/server.cc
)
target_link_libraries(rdma_server polarkv rdmacm ibverbs ibumad pci)

add_executable(
    rpc_client
    simple_test/rpc_client.cc
)
target_link_libraries(rpc_client polarkv rdmacm ibverbs ibumad pci)

add_executable(
    msg_buf
    unit_test/msg_buf.cc
)
target_link_libraries(msg_buf polarkv rdmacm ibverbs ibumad pci)

add_executable(
    hash_table_test
    unit_test/hash_table_test.cc
)
target_link_libraries(hash_table_test polarkv)


add_executable(
    zipf_test
    perf_test/zipf_test.cc
)
target_link_libraries(zipf_test)