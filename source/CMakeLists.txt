include_directories(${PROJECT_SOURCE_DIR}/include)

set(BASE_SOURCE
    kv_engine.cc)

add_library(polarkv STATIC ${BASE_SOURCE})

install(TARGETS polarkv
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
        RUNTIME DESTINATION bin
        PUBLIC_HEADER DESTINATION include)
