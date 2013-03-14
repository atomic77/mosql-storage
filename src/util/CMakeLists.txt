add_library(util STATIC hashtable.c hashtable_itr.c
	opt.c stats.c util.c socket_util.c)

target_link_libraries(util)
