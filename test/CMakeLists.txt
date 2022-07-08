include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/include/util)

add_executable(server server.cc)
target_link_libraries(server polarkv rdmacm ibverbs ibumad pci efa)

add_executable(client client.cc)
target_link_libraries(client polarkv rdmacm ibverbs ibumad pci efa)

add_executable(test_bloom test_bloom.cc)
target_link_libraries(test_bloom polarkv)