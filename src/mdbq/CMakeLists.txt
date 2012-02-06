add_library(mdbq SHARED hub.cpp client.cpp)
TARGET_LINK_LIBRARIES(mdbq mongoclient )
