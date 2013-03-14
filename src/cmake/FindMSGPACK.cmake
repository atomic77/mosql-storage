# - Find libpaxos
# Find the disk-based version of Ring-Paxos on the system
# MSGPACK_INCLUDES - where to find lp_learner.h
# MSGPACK_LIBRARIES - List of libraries when using BDB.
# MSGPACK_FOUND - True if libpaxos found.

set(MSGPACK_ROOT "" CACHE STRING "MessagePack root directory")

find_path(MSGPACK_INCLUDE_DIR msgpack.h HINTS "${MSGPACK_ROOT}/include")
find_library(MSGPACK_LIBRARY libmsgpack.a msgpack HINTS "${MSGPACK_ROOT}/lib")

set(MSGPACK_LIBRARIES ${MSGPACK_LIBRARY})
set(MSGPACK_INCLUDE_DIRS ${MSGPACK_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set LIBXML2_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(MSGPACK DEFAULT_MSG
                                  MSGPACK_LIBRARY MSGPACK_INCLUDE_DIR)

mark_as_advanced(MSGPACK_INCLUDE_DIR MSGPACK_LIBRARY)
