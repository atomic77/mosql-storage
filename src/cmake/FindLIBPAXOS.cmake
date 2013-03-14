# - Find libpaxos
# Find the disk-based version of Ring-Paxos on the system
# LIBPAXOS_INCLUDES - where to find lp_learner.h
# LIBPAXOS_LIBRARIES - List of libraries when using BDB.
# LIBPAXOS_FOUND - True if libpaxos found.

set(LIBPAXOS_ROOT "" CACHE STRING "Ring-Paxos root directory")

# Make this work with UC paxos; the two will have to be merged at some point
find_path(LIBPAXOS_INCLUDE_DIR acceptor_state.h HINTS "${LIBPAXOS_ROOT}/../include")
find_library(LIBPAXOS_LIBRARY paxos HINTS "${LIBPAXOS_ROOT}/lib")

set(LIBPAXOS_LIBRARIES ${LIBPAXOS_LIBRARY})
set(LIBPAXOS_INCLUDE_DIRS ${LIBPAXOS_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set LIBXML2_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(LIBPAXOS DEFAULT_MSG
                                  LIBPAXOS_LIBRARY LIBPAXOS_INCLUDE_DIR)

mark_as_advanced(LIBPAXOS_INCLUDE_DIR LIBPAXOS_LIBRARY)
