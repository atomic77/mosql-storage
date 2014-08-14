
set(GSL_ROOT "" CACHE STRING "GSL root directory")

find_path(GSL_INCLUDE_DIR gsl/gsl_rng.h HINTS "${GSL_ROOT}/include")
find_library(GSL_LIBRARY gsl gslcblas HINTS "${GSL_ROOT}/lib")
# some builds of GSL don't link libgsl to libgslcblas which leads to link errors
find_library(GSL_LIBRARYCBLAS gslcblas HINTS "${GSL_ROOT}/lib")

set(GSL_LIBRARIES ${GSL_LIBRARY} ${GSL_LIBRARYCBLAS})
set(GSL_INCLUDE_DIRS ${GSL_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set LIBXML2_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(GSL DEFAULT_MSG
                                  GSL_LIBRARY GSL_INCLUDE_DIR)

mark_as_advanced(GSL_INCLUDE_DIR GSL_LIBRARY)
