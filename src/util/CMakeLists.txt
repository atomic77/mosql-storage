add_library(util STATIC hashtable.c hashtable_itr.c
	opt.c stats.c util.c socket_util.c carray.c)
include_directories(${LIBEVENT_INCLUDE_DIRS})


target_link_libraries(util)
