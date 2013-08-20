include_directories(${CMAKE_SOURCE_DIR}/util)
include_directories(${CMAKE_SOURCE_DIR}/db)
include_directories(${CMAKE_SOURCE_DIR}/libtapioca)
include_directories(${LIBPAXOS_INCLUDE_DIRS})
include_directories(${LIBEVENT_INCLUDE_DIRS})


set(SOURCE_FILES tstlrg.c example.c pop.c trace.c process_trace.c check_log_consistency.c) # s.cpp sample-stats.cpp trace_stats.cpp)

foreach(p ${SOURCE_FILES})
	get_filename_component(target "${p}" NAME_WE)
	add_executable(${target} ${p})
	target_link_libraries(${target} tapiocadb util tapioca reclib ${TAPIOCA_LINKER_LIBS})
    INSTALL(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/${target} DESTINATION bin)
endforeach(p)

