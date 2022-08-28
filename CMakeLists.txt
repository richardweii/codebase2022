cmake_minimum_required(VERSION 2.8.12)
project(POLAR_CONTEST)

set(CMAKE_BUILD_TYPE "Release")   # 手动指定编译版本
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 11)
set(IPP_LIB "-lippcp")
# set(RDMA_LIB "-lrdmacm -libverbs -libumad -lpci")
set(RDMA_LIB "-lrdmacm -libverbs -libumad -lpci -lefa")
set(CMAKE_CXX_FLAGS "-std=c++11 ${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "-pthread -ldl -lrt ${IPP_LIB} ${RDMA_LIB} -${CMAKE_CXX_FLAGS}")

if( CMAKE_BUILD_TYPE STREQUAL "Debug" )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -Wall -Wno-unused-variable")
else( CMAKE_BUILD_TYPE STREQUAL "Release" )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -fopenmp")
endif( CMAKE_BUILD_TYPE STREQUAL "Debug" )

message("Build Type: ${CMAKE_BUILD_TYPE} ${CMAKE_CXX_FLAGS}")

set(CMAKE_INSTALL_PREFIX ${CMAKE_BINARY_DIR}/polarkv)
MESSAGE("Install prefix is: ${CMAKE_INSTALL_PREFIX}")

include_directories($ENV{CPATH})
add_subdirectory(include)
add_subdirectory(source)
add_subdirectory(test)
