
set(LIBSOURCES tcp_connection.c 
		libtapioca.c mget_result.c tcp_protocol_btree.c libtapioca_btree.c tcp_protocol.c )
add_library(tapioca STATIC ${LIBSOURCES})
add_library(tapioca_shared SHARED ${LIBSOURCES})


IF( CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" )
  SET_TARGET_PROPERTIES(${PROJECT_NAME} PROPERTIES COMPILE_FLAGS "-fPIC")
ENDIF( CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" )


set_target_properties(tapioca_shared PROPERTIES OUTPUT_NAME tapioca)

target_link_libraries(tapioca ${LIBEVENT_LIBRARIES} m rt)
target_link_libraries(tapioca_shared ${LIBEVENT_LIBRARIES} m rt)


INSTALL(FILES ${CMAKE_SOURCE_DIR}/libtapioca/tapioca.h DESTINATION include/tapioca)
INSTALL(FILES ${CMAKE_SOURCE_DIR}/libtapioca/tapioca_btree.h DESTINATION include/tapioca)
INSTALL(FILES ${CMAKE_SOURCE_DIR}/libtapioca/mget_result.h DESTINATION include/tapioca)
INSTALL(TARGETS tapioca tapioca_shared DESTINATION lib)