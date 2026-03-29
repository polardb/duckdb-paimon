cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME paimon)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

# Import paimon-cpp as a subproject
include(ExternalProject)

set(PAIMON_CPP_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party/paimon-cpp)
set(PAIMON_CPP_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/paimon-cpp-install)
set(PAIMON_CPP_INCLUDE ${PAIMON_CPP_PREFIX}/include)
set(PAIMON_CPP_LIB ${PAIMON_CPP_PREFIX}/lib64)
file(MAKE_DIRECTORY ${PAIMON_CPP_INCLUDE})

set(PAIMON_CPP_PATCHES
  ${CMAKE_CURRENT_SOURCE_DIR}/patches/paimon-cpp-install-arrow-headers.patch
  ${CMAKE_CURRENT_SOURCE_DIR}/patches/paimon-cpp-link-arrow-brotli-libs.patch
  ${CMAKE_CURRENT_SOURCE_DIR}/patches/paimon-cpp-disable-glog-unwind.patch)

# Build patch command: apply each patch idempotently
set(PAIMON_CPP_PATCH_SCRIPT "")
foreach(patch IN LISTS PAIMON_CPP_PATCHES)
  string(APPEND PAIMON_CPP_PATCH_SCRIPT
    "git -C ${PAIMON_CPP_SOURCE_DIR} apply --reverse --check ${patch} 2>/dev/null || git -C ${PAIMON_CPP_SOURCE_DIR} apply ${patch} && ")
endforeach()
string(APPEND PAIMON_CPP_PATCH_SCRIPT "true")

ExternalProject_Add(paimon_cpp_ep
  SOURCE_DIR ${PAIMON_CPP_SOURCE_DIR}
  PATCH_COMMAND
    sh -c "${PAIMON_CPP_PATCH_SCRIPT}"
  CMAKE_ARGS
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_INSTALL_PREFIX=${PAIMON_CPP_PREFIX}
    -DCMAKE_INSTALL_LIBDIR=lib64
    -DLIBUNWIND_LIBRARY=FALSE
    -DPAIMON_BUILD_TESTS=OFF
    -DPAIMON_BUILD_SHARED=ON
    -DPAIMON_BUILD_STATIC=OFF
    -DPAIMON_ENABLE_JINDO=ON
    -DPAIMON_ENABLE_ORC=ON
    -DPAIMON_ENABLE_AVRO=ON
    -DPAIMON_ENABLE_LUMINA=OFF
    -DPAIMON_ENABLE_LUCENE=OFF
  BUILD_BYPRODUCTS
    ${PAIMON_CPP_LIB}/libpaimon.so
    ${PAIMON_CPP_LIB}/libpaimon_local_file_system.so
    ${PAIMON_CPP_LIB}/libpaimon_jindo_file_system.so
    ${PAIMON_CPP_LIB}/libjindosdk_c.so
    ${PAIMON_CPP_LIB}/libpaimon_parquet_file_format.so
    ${PAIMON_CPP_LIB}/libpaimon_orc_file_format.so
    ${PAIMON_CPP_LIB}/libpaimon_avro_file_format.so
    ${PAIMON_CPP_LIB}/libpaimon_blob_file_format.so
)

# Extension sources
set(EXTENSION_SOURCES
  src/paimon_extension.cpp
  src/paimon_functions.cpp
  src/paimon_storage/paimon_scan.cpp
  src/paimon_storage/paimon_catalog.cpp
  src/paimon_storage/paimon_schema_entry.cpp
  src/paimon_storage/paimon_schema_set.cpp
  src/paimon_storage/paimon_table_entry.cpp
  src/paimon_storage/paimon_table_set.cpp
  src/paimon_storage/paimon_transaction_manager.cpp
  src/paimon_storage/paimon_type_utils.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Include paimon-cpp and arrow headers
target_include_directories(${EXTENSION_NAME} SYSTEM PRIVATE ${PAIMON_CPP_INCLUDE})
target_include_directories(${TARGET_NAME}_loadable_extension SYSTEM PRIVATE ${PAIMON_CPP_INCLUDE})

# Build paimon-cpp before building the extension
add_dependencies(${EXTENSION_NAME} paimon_cpp_ep)
add_dependencies(${TARGET_NAME}_loadable_extension paimon_cpp_ep)

# Link with paimon-cpp
set(PAIMON_CPP_LIBS
  ${PAIMON_CPP_LIB}/libpaimon.so
  -Wl,--no-as-needed
  ${PAIMON_CPP_LIB}/libpaimon_local_file_system.so
  ${PAIMON_CPP_LIB}/libpaimon_jindo_file_system.so
  ${PAIMON_CPP_LIB}/libjindosdk_c.so
  ${PAIMON_CPP_LIB}/libpaimon_parquet_file_format.so
  ${PAIMON_CPP_LIB}/libpaimon_orc_file_format.so
  ${PAIMON_CPP_LIB}/libpaimon_avro_file_format.so
  ${PAIMON_CPP_LIB}/libpaimon_blob_file_format.so
  -Wl,--as-needed
)
target_link_libraries(${EXTENSION_NAME} ${PAIMON_CPP_LIBS})
target_link_libraries(${TARGET_NAME}_loadable_extension ${PAIMON_CPP_LIBS})

# Bundle paimon-cpp runtime dependencies next to paimon.duckdb_extension.
set(PAIMON_CPP_RUNTIME_BUNDLE_LIBS
  ${PAIMON_CPP_LIB}/libpaimon.so
  ${PAIMON_CPP_LIB}/libpaimon_local_file_system.so
  ${PAIMON_CPP_LIB}/libpaimon_jindo_file_system.so
  ${PAIMON_CPP_LIB}/libjindosdk_c.so.6
  ${PAIMON_CPP_LIB}/libpaimon_parquet_file_format.so
  ${PAIMON_CPP_LIB}/libpaimon_orc_file_format.so
  ${PAIMON_CPP_LIB}/libpaimon_avro_file_format.so
  ${PAIMON_CPP_LIB}/libpaimon_blob_file_format.so
)
foreach(lib IN LISTS PAIMON_CPP_RUNTIME_BUNDLE_LIBS)
  add_custom_command(
    TARGET ${TARGET_NAME}_loadable_extension
    POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
      ${lib}
      $<TARGET_FILE_DIR:${TARGET_NAME}_loadable_extension>
  )
endforeach()

# Make runtime library lookup relocatable for the shipped loadable extension.
set_target_properties(${TARGET_NAME}_loadable_extension PROPERTIES
  BUILD_RPATH "\$ORIGIN"
  INSTALL_RPATH "\$ORIGIN"
  BUILD_WITH_INSTALL_RPATH ON
  INSTALL_RPATH_USE_LINK_PATH OFF
)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
