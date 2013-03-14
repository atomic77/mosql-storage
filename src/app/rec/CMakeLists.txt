include_directories(${CMAKE_SOURCE_DIR}/db)
include_directories(${CMAKE_SOURCE_DIR}/util)
include_directories(${LIBPAXOS_INCLUDE_DIRS})
include_directories(${LIBEVENT_INCLUDE_DIRS})
include_directories(${BDB_INCLUDE_DIRS})
include_directories(${MSGPACK_INCLUDE_DIRS})

add_executable(rec rec.c index.c)

target_link_libraries(rec tapiocadb ${TAPIOCA_LINKER_LIBS} ${MSGPACK_LIBRARIES})

GET_TARGET_PROPERTY(REC_BIN rec LOCATION)

ADD_CUSTOM_COMMAND(TARGET rec POST_BUILD
          COMMAND ${CMAKE_COMMAND} -E copy ${REC_BIN} ${PROJECT_BINARY_DIR}/bin)
