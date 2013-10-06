include_directories(${CMAKE_SOURCE_DIR}/util)
include_directories(${CMAKE_SOURCE_DIR}/db)
include_directories(${CMAKE_SOURCE_DIR}/libtapioca)
include_directories(${CMAKE_SOURCE_DIR}/app/tapioca)
include_directories(${CMAKE_SOURCE_DIR}/app/rec)
include_directories(${CMAKE_SOURCE_DIR}/app/cm)
include_directories(${LIBEVENT_INCLUDE_DIRS})
include_directories(${LIBPAXOS_INCLUDE_DIRS})
include_directories(${LIBUUID_INCLUDE_DIRS})
include_directories(${MSGPACK_INCLUDE_DIRS})

SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fpermissive")
add_executable(mosql_gtest_main gtest_main.cc test_helpers.cc storage_unittest.cc 
	vset_unittest.cc transaction_unittest.cc tapioca_unittest.cc remote_mock.c 
	dump_unittest.cc contention_unittest.cc 
	bptree_basic_unittest.cc bptree_interface_unittest.cc bptree_concurrent_unittest.cc
	)

add_library(gtest STATIC gtest-all.cc)

target_link_libraries(mosql_gtest_main gtest bplustree tapioca tapiocadb ${TAPIOCA_LINKER_LIBS} ${PAXOS_LINKER_LIBS} ${LIBUUID_LIBRARIES} ${MSGPACK_LIBRARIES}) 

INSTALL(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/mosql_gtest_main DESTINATION mosql-test)
