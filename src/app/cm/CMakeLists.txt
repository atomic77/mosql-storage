include_directories(${CMAKE_SOURCE_DIR}/db)
include_directories(${CMAKE_SOURCE_DIR}/util)
include_directories(${LIBPAXOS_INCLUDE_DIRS})

add_executable(cm cm.c bloom.c msg.c queue.c validation_fast.c)

target_link_libraries(cm tapiocadb ${TAPIOCA_LINKER_LIBS})

GET_TARGET_PROPERTY(CM_BIN cm LOCATION)

ADD_CUSTOM_COMMAND(TARGET cm POST_BUILD
          COMMAND ${CMAKE_COMMAND} -E copy ${CM_BIN} ${PROJECT_BINARY_DIR}/bin)
