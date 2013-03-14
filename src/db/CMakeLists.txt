include_directories(${CMAKE_SOURCE_DIR}/util)
include_directories(${LIBPAXOS_INCLUDE_DIRS})
include_directories(${BDB_INCLUDE_DIRS})
include_directories(${LIBEVENT_INCLUDE_DIRS})

add_library(tapiocadb STATIC config.c config_reader.c cproxy.c
	debug_malloc.c hash.c keyval_alloc.c peer.c remote.c sm.c 
	storage.c  tapiocadb.c transaction.c vset_array.c
	vset_array_cache.c vset_array_sorted.c vset_list.c)

target_link_libraries(tapiocadb util)
target_link_libraries(tapiocadb ${LIBPAXOS_LIBRARIES} ${LIBEVENT_LIBRARIES} ${BDB_LIBRARIES})
