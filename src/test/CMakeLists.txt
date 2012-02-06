INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/..)
#CMAKE_FORCE_CXX_COMPILER(g++ GNU)

set(PROJECT_TEST_NAME mdbq_test)
include_directories(${CMAKE_BINARY_DIR}/src)

file(GLOB TEST_SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
add_executable(mdbq_test ${TEST_SRC_FILES})
target_link_libraries(mdbq_test  ${Boost_LIBRARIES} pthread mdbq)

add_test(gtest ${PROJECT_TEST_NAME})

