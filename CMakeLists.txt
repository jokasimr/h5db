cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME h5db)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
# For clang-tidy builds, HDF5 is optional (only needs headers for static analysis)
if(CLANG_TIDY)
    find_package(HDF5 COMPONENTS C)
else()
    find_package(HDF5 REQUIRED COMPONENTS C)
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)
if(HDF5_FOUND)
    include_directories(${HDF5_INCLUDE_DIRS})
endif()

# Require C++17 for std::variant
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(EXTENSION_SOURCES
    src/h5db_extension.cpp
    src/h5_common.cpp
    src/h5_tree.cpp
    src/h5_read.cpp
    src/h5_attributes.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link HDF5 in both the static library as the loadable extension
if(HDF5_FOUND)
    target_link_libraries(${EXTENSION_NAME} ${HDF5_C_LIBRARIES})
    target_link_libraries(${LOADABLE_EXTENSION_NAME} ${HDF5_C_LIBRARIES})
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
