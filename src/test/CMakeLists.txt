include_directories(${CMAKE_SOURCE_DIR}/util)
include_directories(${CMAKE_SOURCE_DIR}/libtapioca)

set(PROGRAMS example.c pop.c trace.c process_trace.c s.cpp sample-stats.cpp trace_stats.cpp)

foreach(p ${PROGRAMS})
	get_filename_component(target "${p}" NAME)
	add_executable(${target} ${p})
	target_link_libraries(${target} util tapioca ${TAPIOCA_LINKER_LIBS})
endforeach(p)

