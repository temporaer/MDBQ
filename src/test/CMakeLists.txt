# ---------- Find Boost Headers/Libraries -----------------------
SET (Boost_FIND_REQUIRED TRUE)
SET (Boost_FIND_QUIETLY TRUE)
SET (Boost_USE_MULTITHREADED TRUE)
SET (Boost_USE_STATIC_LIBS FALSE)
SET (Boost_ADDITIONAL_VERSIONS "1.39" "1.39.0" "1.40" "1.42" "1.43" "1.44" "1.45" "1.46" "1.47" "1.48" )
FIND_PACKAGE( Boost 1.37 COMPONENTS date_time program_options filesystem thread system REQUIRED )
INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIRS})
LINK_DIRECTORIES(${Boost_LIBRARY_DIRS})


INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/..)
find_package(GTest REQUIRED)
#CMAKE_FORCE_CXX_COMPILER(g++ GNU)

set(PROJECT_TEST_NAME mdbq_test)
include_directories(${GTEST_INCLUDE_DIRS})
include_directories(${CMAKE_BINARY_DIR}/src)

file(GLOB TEST_SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
add_executable(mdbq_test ${TEST_SRC_FILES})
target_link_libraries(mdbq_test  ${GTEST_BOTH_LIBRARIES} ${Boost_LIBRARIES} pthread mdbq)

add_test(gtest ${PROJECT_TEST_NAME})

