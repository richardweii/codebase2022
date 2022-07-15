include_directories(${PROJECT_SOURCE_DIR}/include)

add_executable(
    rdma_client
    client.cc
)
target_link_libraries(rdma_client polarkv rdmacm ibverbs ibumad pci)

add_executable(
    rdma_server
    server.cc
)
target_link_libraries(rdma_server polarkv rdmacm ibverbs ibumad pci)

add_executable(
    perf_client
    perf_client.cc
)
target_link_libraries(perf_client polarkv rdmacm ibverbs ibumad pci)

add_executable(
    msg_buf
    msg_buf.cc
)
target_link_libraries(msg_buf polarkv rdmacm ibverbs ibumad pci)
