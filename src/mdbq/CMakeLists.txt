add_library(mdbq SHARED hub.cpp client.cpp)
TARGET_LINK_LIBRARIES(mdbq mongoclient )
set_target_properties(mdbq PROPERTIES
      PUBLIC_HEADER "hub.hpp;client.hpp")
INSTALL(
    TARGETS mdbq
    EXPORT MDBQLibraryDepends
    RUNTIME DESTINATION "${INSTALL_BIN_DIR}" COMPONENT bin
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}" COMPONENT shlib
    PUBLIC_HEADER DESTINATION "${INSTALL_INCLUDE_DIR}/mdbq" COMPONENT dev)
