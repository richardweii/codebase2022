include_directories(${PROJECT_SOURCE_DIR}/include)

file(GLOB BASE_SOURCE ${PROJECT_SOURCE_DIR}/source/*.cc)
message("${BASE_SOURCE}")

add_library(polarkv STATIC ${BASE_SOURCE})

install(TARGETS polarkv
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
        RUNTIME DESTINATION bin
        PUBLIC_HEADER DESTINATION include)
