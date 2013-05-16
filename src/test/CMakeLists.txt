include_directories(${CMAKE_SOURCE_DIR}/util)
include_directories(${CMAKE_SOURCE_DIR}/libtapioca)

set(SOURCE_FILES tstlrg.c example.c pop.c trace.c process_trace.c) # s.cpp sample-stats.cpp trace_stats.cpp)

foreach(p ${SOURCE_FILES})
	get_filename_component(target "${p}" NAME_WE)
	add_executable(${target} ${p})
	target_link_libraries(${target} util tapioca ${TAPIOCA_LINKER_LIBS})
    INSTALL(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/${target} DESTINATION bin)
endforeach(p)

