include_directories(${CMAKE_SOURCE_DIR}/db)
include_directories(${CMAKE_SOURCE_DIR}/util)
include_directories(${LIBPAXOS_INCLUDE_DIRS})

add_executable(cm cm.c bloom.c msg.c queue.c validation_fast.c)

target_link_libraries(cm tapiocadb ${TAPIOCA_LINKER_LIBS})

INSTALL(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/cm DESTINATION bin)