include_directories(${PROCJECT_SOURCE_DIR}/include)
file(GLOB BASE_INCLUDE ${PROJECT_SOURCE_DIR}/include/*.h)

install(FILES   ${BASE_INCLUDE}
    DESTINATION include)