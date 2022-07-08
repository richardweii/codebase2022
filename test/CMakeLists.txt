include_directories(${PROJECT_SOURCE_DIR}/include)

set(SOURCE_PATH ${PROJECT_SOURCE_DIR}/source)

add_executable(test_logging test_logging.cc ${SOURCE_PATH}/util/logging.cc)
target_link_libraries(test_logging gtest_main)
