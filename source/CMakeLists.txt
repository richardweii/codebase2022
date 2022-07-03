include_directories(${PROJECT_SOURCE_DIR}/include)

aux_source_directory(${PROJECT_SOURCE_DIR}/source SRC)
aux_source_directory(${PROJECT_SOURCE_DIR}/source/util SRC)

add_library(polarkv STATIC ${SRC})

install(TARGETS polarkv
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
        RUNTIME DESTINATION bin
        PUBLIC_HEADER DESTINATION include)
