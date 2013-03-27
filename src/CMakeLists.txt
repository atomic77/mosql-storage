cmake_minimum_required(VERSION 2.6)
project(tapioca)
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

find_package(BDB REQUIRED)
find_package(LIBEVENT REQUIRED)
find_package(LIBPAXOS REQUIRED)
find_package(LIBUUID REQUIRED)
find_package(MSGPACK REQUIRED)

# System libraries
set(TAPIOCA_LINKER_LIBS ${TAPIOCA_LINKER_LIBS} pthread)
if(NOT APPLE)
	set(TAPIOCA_LINKER_LIBS ${TAPIOCA_LINKER_LIBS} m rt pthread)
endif()

add_subdirectory(util)
add_subdirectory(db)
add_subdirectory(libtapioca)
add_subdirectory(app/cm)
add_subdirectory(app/rec)
add_subdirectory(app/shell)
add_subdirectory(app/tapioca)
add_subdirectory(test)
add_subdirectory(unit)

# Copy all the bin files we will need into build/bin for convenient use
set(BIN_FILES app/tapioca/tapioca app/cm/cm app/rec/rec app/shell/tshell) 

#file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/bin)

ADD_CUSTOM_COMMAND(TARGET binarydir
 POST_BUILD
 COMMAND ${CMAKE_COMMAND} -E make_directory ${PROJECT_BINARY_DIR}/bin)


#file(COPY ${LIBPAXOS_ROOT}/ring-paxos/acceptor DESTINATION ${PROJECT_BINARY_DIR}/bin)
#file(COPY ${LIBPAXOS_ROOT}/tests/example_acceptor DESTINATION ${PROJECT_BINARY_DIR}/bin)
#file(RENAME ${PROJECT_BINARY_DIR}/bin/example_acceptor ${PROJECT_BINARY_DIR}/bin/acceptor)

file(GLOB ROOT_SCRIPTS ../*.sh)
file(COPY ${ROOT_SCRIPTS} DESTINATION ${PROJECT_BINARY_DIR}/)

file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/script)
file(COPY ../script/launchers.sh DESTINATION ${PROJECT_BINARY_DIR}/script)

file(GLOB CONFIG_FILES ../config/*.cfg)
file(COPY ${CONFIG_FILES} DESTINATION ${PROJECT_BINARY_DIR}/config)
