include_directories(${CMAKE_SOURCE_DIR}/db)
include_directories(${CMAKE_SOURCE_DIR}/util)
include_directories(${LIBEVENT_INCLUDE_DIRS})
include_directories(${MSGPACK_INCLUDE_DIRS})
include_directories(${LIBUUID_INCLUDE_DIRS})

add_executable(tapioca_executable tapioca.c tcp.c  bplustree.c bplustree_client.c bplustree_debug.c btree.c tpl.c)
SET_TARGET_PROPERTIES(tapioca_executable PROPERTIES OUTPUT_NAME "tapioca")

target_link_libraries(tapioca_executable tapiocadb ${TAPIOCA_LINKER_LIBS}  ${LIBUUID_LIBRARIES} ${MSGPACK_LIBRARIES})

GET_TARGET_PROPERTY(TAPIOCA_BIN tapioca_executable LOCATION)

ADD_CUSTOM_COMMAND(TARGET tapioca_executable POST_BUILD
          COMMAND ${CMAKE_COMMAND} -E copy ${TAPIOCA_BIN} ${PROJECT_BINARY_DIR}/bin)
