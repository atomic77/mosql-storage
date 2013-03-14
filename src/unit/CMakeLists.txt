include_directories(${CMAKE_SOURCE_DIR}/util)
include_directories(${CMAKE_SOURCE_DIR}/db)
include_directories(${CMAKE_SOURCE_DIR}/libtapioca)
include_directories(${LIBEVENT_INCLUDE_DIRS})

add_executable(gtest_main gtest_main.cc test_helpers.cc storage_unittest.cc 
	vset_unittest.cc transaction_unittest.cc tapioca_unittest.cc remote_mock.c 
	dump_unittest.cc contention_unittest.cc )

add_library(gtest STATIC gtest-all.cc)

target_link_libraries(gtest_main gtest tapioca tapiocadb ${TAPIOCA_LINKER_LIBS})
