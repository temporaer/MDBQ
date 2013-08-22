add_library(mdbq SHARED hub.cpp client.cpp)
TARGET_LINK_LIBRARIES(mdbq mongoclient ${Boost_LIBRARIES})
set_target_properties(mdbq PROPERTIES
      PUBLIC_HEADER "hub.hpp;client.hpp")
INSTALL(
    TARGETS mdbq
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    PUBLIC_HEADER DESTINATION include/mdbq)
