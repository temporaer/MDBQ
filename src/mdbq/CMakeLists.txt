add_library(mdbq SHARED hub.cpp client.cpp)
TARGET_LINK_LIBRARIES(mdbq mongoclient ${Boost_LIBRARIES} )
INSTALL(
    TARGETS mdbq
    DESTINATION lib)
INSTALL(
    FILES hub.hpp client.hpp
    DESTINATION include/mdbq
)
