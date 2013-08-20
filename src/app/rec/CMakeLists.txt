include_directories(${CMAKE_SOURCE_DIR}/db)
include_directories(${CMAKE_SOURCE_DIR}/util)
include_directories(${LIBPAXOS_INCLUDE_DIRS})
include_directories(${LIBEVENT_INCLUDE_DIRS})
include_directories(${BDB_INCLUDE_DIRS})
include_directories(${MSGPACK_INCLUDE_DIRS})

add_executable(rec rec.c index.c)
add_library(reclib STATIC rec.c index.c)

target_link_libraries(rec tapiocadb util ${TAPIOCA_LINKER_LIBS} ${MSGPACK_LIBRARIES} )
target_link_libraries(reclib tapiocadb util ${TAPIOCA_LINKER_LIBS} ${MSGPACK_LIBRARIES} )

INSTALL(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/rec DESTINATION bin)
