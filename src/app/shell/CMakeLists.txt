include_directories(${CMAKE_SOURCE_DIR}/util)
include_directories(${CMAKE_SOURCE_DIR}/libtapioca)

add_executable(tshell tshell.c)

target_link_libraries(tshell util tapioca readline ${TAPIOCA_LINKER_LIBS})

GET_TARGET_PROPERTY(TSHELL_BIN tshell LOCATION)

ADD_CUSTOM_COMMAND(TARGET tshell POST_BUILD
          COMMAND ${CMAKE_COMMAND} -E copy ${TSHELL_BIN} ${PROJECT_BINARY_DIR}/bin)
