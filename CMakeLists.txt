cmake_minimum_required( VERSION 2.6 FATAL_ERROR )
PROJECT(mdbq CXX)

#SET(CPACK_SOURCE_GENERATOR "TGZ")
SET(CPACK_GENERATOR "DEB")
SET(CPACK_PACKAGE_NAME "mdbq")
SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY "MongoDB Message Queue in C++")
SET(CPACK_PACKAGE_CONTACT "Hannes Schulz <schulz at ais dot uni-bonn dot de>")
SET(CPACK_PACKAGE_DESCRIPTION_FILE "${CMAKE_CURRENT_SOURCE_DIR}/README.markdown")
SET(CPACK_PACKAGE_VERSION_MAJOR "0")
SET(CPACK_PACKAGE_VERSION_MINOR "1")
SET(CPACK_PACKAGE_VERSION_PATCH ${DATESTR})
SET(CPACK_SOURCE_IGNORE_FILES "${CPACK_SOURCE_IGNORE_FILES};${CMAKE_SOURCE_DIR}/.git")
SET(CPACK_PACKAGE_INSTALL_DIRECTORY "mdbq")


# Offer the user the choice of overriding the installation directories
set(INSTALL_LIB_DIR lib CACHE PATH "Installation directory for libraries")
set(INSTALL_BIN_DIR bin CACHE PATH "Installation directory for executables")
set(INSTALL_INCLUDE_DIR include CACHE PATH "Installation directory for header files")
if(WIN32 AND NOT CYGWIN)
  set(DEF_INSTALL_CMAKE_DIR CMake)
else()
    set(DEF_INSTALL_CMAKE_DIR lib/CMake/MDBQ)
endif()
set(INSTALL_CMAKE_DIR ${DEF_INSTALL_CMAKE_DIR} CACHE PATH "Installation directory for CMake files")
# Make relative paths absolute (needed later on)
foreach(p LIB BIN INCLUDE CMAKE)
  set(var INSTALL_${p}_DIR)
  if(NOT IS_ABSOLUTE "${${var}}")
    set(${var} "${CMAKE_INSTALL_PREFIX}/${${var}}")
  endif()
endforeach()

SET(CPACK_DEBIAN_PACKAGE_DEPENDS    "libboost-dev (> 1.42.0)" )
INCLUDE(CPack)

FIND_PACKAGE(Doxygen)
IF(DOXYGEN_FOUND)
	CONFIGURE_FILE(${CMAKE_SOURCE_DIR}/doc/Doxyfile.in ${CMAKE_BINARY_DIR}/doc/Doxyfile)
	ADD_CUSTOM_COMMAND(
		DEPENDS ${CMAKE_BINARY_DIR}/doc/Doxyfile
		OUTPUT  ${CMAKE_BINARY_DIR}/doc/html/index.html
		COMMAND ${DOXYGEN_EXECUTABLE} ${CMAKE_BINARY_DIR}/doc/Doxyfile
		WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
		)
	ADD_CUSTOM_TARGET(doc DEPENDS ${CMAKE_BINARY_DIR}/doc/html/index.html)
ENDIF(DOXYGEN_FOUND)

# ---------- Find Boost Headers/Libraries -----------------------
SET (Boost_FIND_REQUIRED TRUE)
SET (Boost_FIND_QUIETLY TRUE)
SET (Boost_USE_MULTITHREADED TRUE)
SET (Boost_USE_STATIC_LIBS FALSE)
SET (Boost_ADDITIONAL_VERSIONS "1.39" "1.39.0" "1.40" "1.42" "1.43" "1.44" "1.45" "1.46" "1.47" "1.48" )
FIND_PACKAGE( Boost 1.46 COMPONENTS unit_test_framework date_time program_options filesystem thread system REQUIRED )
INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIRS})
LINK_DIRECTORIES(${Boost_LIBRARY_DIRS})

ENABLE_TESTING()
add_subdirectory(src)


# The interesting stuff goes here
# ===============================
 
# Add all targets to the build-tree export set
export(TARGETS mdbq
  FILE "${PROJECT_BINARY_DIR}/MDBQLibraryDepends.cmake")
 
# Export the package for use from the build-tree
# (this registers the build-tree with a global CMake-registry)
export(PACKAGE MDBQ)
 
# Create a MDBQBuildTreeSettings.cmake file for the use from the build tree
configure_file(MDBQBuildTreeSettings.cmake.in
  "${PROJECT_BINARY_DIR}/MDBQBuildTreeSettings.cmake" @ONLY)
 
# Create the MDBQConfig.cmake and MDBQConfigVersion files
file(RELATIVE_PATH CONF_REL_INCLUDE_DIR "${INSTALL_CMAKE_DIR}"
   "${INSTALL_INCLUDE_DIR}")
configure_file(MDBQConfig.cmake.in
  "${PROJECT_BINARY_DIR}/MDBQConfig.cmake" @ONLY)
configure_file(MDBQConfigVersion.cmake.in
  "${PROJECT_BINARY_DIR}/MDBQConfigVersion.cmake" @ONLY)
 
# Install the MDBQConfig.cmake and MDBQConfigVersion.cmake
install(FILES
  "${PROJECT_BINARY_DIR}/MDBQConfig.cmake"
  "${PROJECT_BINARY_DIR}/MDBQConfigVersion.cmake"
  DESTINATION "${INSTALL_CMAKE_DIR}" COMPONENT dev)

# Install the export set for use with the install-tree
install(EXPORT MDBQLibraryDepends DESTINATION
  "${INSTALL_CMAKE_DIR}" COMPONENT dev)
