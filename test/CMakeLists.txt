include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/test)

# simple test
add_executable(
    rdma_client
    simple_test/client.cc
)
target_link_libraries(rdma_client polarkv rdmacm ibverbs ibumad pci ippcp)

add_executable(
    rdma_server
    simple_test/server.cc
)

# rpc test
target_link_libraries(rdma_server polarkv rdmacm ibverbs ibumad pci ippcp)

add_executable(
    rpc_client
    simple_test/rpc_client.cc
)
target_link_libraries(rpc_client polarkv rdmacm ibverbs ibumad pci ippcp)

# unit test
add_executable(
    msg_buf
    unit_test/msg_buf.cc
)
target_link_libraries(msg_buf polarkv rdmacm ibverbs ibumad pci ippcp)

add_executable(
    hash_table_test
    unit_test/hash_table_test.cc
    ${PROJECT_SOURCE_DIR}/source/util/hash.cc
)
target_link_libraries(hash_table_test)


add_executable(
    zipf_test
    perf_test/zipf_test.cc
)
target_link_libraries(zipf_test)

# perf test
add_executable(
    perf_client
    perf_test/client.cc
)
target_link_libraries(perf_client polarkv rdmacm ibverbs ibumad pci ippcp)


add_executable(
    perf_server
    perf_test/server.cc
)
target_link_libraries(perf_server polarkv rdmacm ibverbs ibumad pci ippcp)