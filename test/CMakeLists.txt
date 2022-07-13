include_directories(${PROJECT_SOURCE_DIR}/include)

add_executable(
    rdma_client
    client.cc
)
target_link_libraries(rdma_client polarkv rdmacm ibverbs ibumad pci efa)

add_executable(
    rdma_server
    server.cc
)
target_link_libraries(rdma_server polarkv rdmacm ibverbs ibumad pci efa)


