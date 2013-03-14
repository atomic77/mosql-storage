#include_directories(${LIBEVENT_INCLUDE_DIRS})
include_directories(/home/sciascid/sw/include)

add_library(tapioca STATIC tcp_connection.c tcp_protocol.c 
		libtapioca.c mget_result.c)

IF( CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" )
  SET_TARGET_PROPERTIES(${PROJECT_NAME} PROPERTIES COMPILE_FLAGS "-fPIC")
ENDIF( CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" )

target_link_libraries(tapioca ${LIBEVENT_LIBRARIES})
