file(GLOB BASE_INCLUDE ${PROJECT_SOURCE_DIR}/include/*.h)
file(GLOB UTIL_INCLUDE ${PROJECT_SOURCE_DIR}/include/util/*.h)

message("${BASE_INCLUDE}")

install(FILES   ${BASE_INCLUDE}
    DESTINATION include)
install (FILES ${UTIL_INCLUDE} DESTINATION include/util)