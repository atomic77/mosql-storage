include_directories(${CMAKE_SOURCE_DIR}/util)
include_directories(${CMAKE_SOURCE_DIR}/libtapioca)

add_executable(tshell tshell.c)

target_link_libraries(tshell util tapioca readline ${TAPIOCA_LINKER_LIBS})


INSTALL(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/tshell DESTINATION bin)