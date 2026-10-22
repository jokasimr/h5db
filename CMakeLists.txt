cmake_minimum_required(VERSION 3.14...3.29)

# Set extension name here
set(TARGET_NAME h5db)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be
# added in ./vcpkg.json and then used in cmake with find_package. Feel free to
# remove or replace with other dependencies. Note that it should also be removed
# from vcpkg.json to prevent needlessly installing it..
find_package(HDF5 REQUIRED COMPONENTS C)

find_package(libssh2 CONFIG REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)

project(${TARGET_NAME})
include_directories(src/include)
if(HDF5_FOUND)
  include_directories(${HDF5_INCLUDE_DIRS})
endif()

# Require C++20 for std::atomic::wait.
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)

set(EXTENSION_SOURCES
    src/h5db_extension.cpp
    src/h5_common.cpp
    src/h5_sftp_secrets.cpp
    src/h5_remote_backend.cpp
    src/h5_remote_vfd.cpp
    src/h5_tree_shared.cpp
    src/h5_first_file.cpp
    src/h5_attr.cpp
    src/h5_tree.cpp
    src/h5_ls.cpp
    src/h5_read_shared.cpp
    src/h5_read_table.cpp
    src/h5_read_scalar.cpp
    src/h5_attributes.cpp)

build_extension_library(${TARGET_NAME} ${EXTENSION_SOURCES})

# The loadable extension links this archive and inherits its dependencies.
target_link_libraries(${EXTENSION_NAME} ${HDF5_C_LIBRARIES} libssh2::libssh2)
if(WIN32)
  target_link_libraries(${EXTENSION_NAME} ws2_32)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
