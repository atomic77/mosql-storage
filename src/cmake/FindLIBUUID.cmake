# - Find libpaxos
# Find the disk-based version of Ring-Paxos on the system
# LIBUUID_INCLUDES - where to find lp_learner.h
# LIBUUID_LIBRARIES - List of libraries when using BDB.
# LIBUUID_FOUND - True if libpaxos found.

set(LIBUUID_ROOT "" CACHE STRING "e2fsprogs or libuuid root directory")

find_path(LIBUUID_INCLUDE_DIR uuid/uuid.h HINTS "${LIBUUID_ROOT}/include")
find_library(LIBUUID_LIBRARY libuuid.a uuid HINTS "${LIBUUID_ROOT}/lib")

set(LIBUUID_LIBRARIES ${LIBUUID_LIBRARY})
set(LIBUUID_INCLUDE_DIRS ${LIBUUID_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set LIBXML2_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(LIBUUID DEFAULT_MSG
                                  LIBUUID_LIBRARY LIBUUID_INCLUDE_DIR)

mark_as_advanced(LIBUUID_INCLUDE_DIR LIBUUID_LIBRARY)
