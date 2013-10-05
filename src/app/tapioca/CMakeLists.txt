include_directories(${CMAKE_SOURCE_DIR}/libtapioca)
include_directories(${CMAKE_SOURCE_DIR}/db)
include_directories(${CMAKE_SOURCE_DIR}/util)
include_directories(${LIBEVENT_INCLUDE_DIRS})
include_directories(${LIBPAXOS_INCLUDE_DIRS})
include_directories(${MSGPACK_INCLUDE_DIRS})
include_directories(${LIBUUID_INCLUDE_DIRS})

add_executable(tapioca_executable tapioca.c tcp.c  bplustree.c bplustree_client.c bplustree_debug.c)
add_library(bplustree bplustree.c bplustree_client.c bplustree_debug.c )
SET_TARGET_PROPERTIES(tapioca_executable PROPERTIES OUTPUT_NAME "tapioca")

target_link_libraries(tapioca_executable tapiocadb ${TAPIOCA_LINKER_LIBS}  ${LIBUUID_LIBRARIES} ${MSGPACK_LIBRARIES} )

INSTALL(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/tapioca DESTINATION bin)
