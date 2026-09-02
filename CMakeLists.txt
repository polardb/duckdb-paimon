cmake_minimum_required(VERSION 3.17)

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

option(PAIMON_ENABLE_OSS "Whether to enable the OSS file system" ON)
option(PAIMON_ENABLE_S3 "Whether to enable the S3 file system" ON)
option(PAIMON_ENABLE_REST "Whether to enable the REST catalog" ON)

set(PAIMON_CPP_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party/paimon-cpp)
set(PAIMON_CPP_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/paimon-cpp-install)
set(PAIMON_CPP_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/paimon-cpp-build)
set(PAIMON_CPP_INCLUDE ${PAIMON_CPP_PREFIX}/include)
set(PAIMON_CPP_LIB ${PAIMON_CPP_PREFIX}/lib64)
file(MAKE_DIRECTORY ${PAIMON_CPP_INCLUDE})

set(PAIMON_CPP_PATCHES
  ${CMAKE_CURRENT_SOURCE_DIR}/patches/paimon-cpp-install-arrow-headers.patch)

# Build patch command: apply each patch idempotently
set(PAIMON_CPP_PATCH_SCRIPT "")
foreach(patch IN LISTS PAIMON_CPP_PATCHES)
  string(APPEND PAIMON_CPP_PATCH_SCRIPT
    "git -C ${PAIMON_CPP_SOURCE_DIR} apply --reverse --check ${patch} 2>/dev/null || git -C ${PAIMON_CPP_SOURCE_DIR} apply ${patch} && ")
endforeach()
string(APPEND PAIMON_CPP_PATCH_SCRIPT "true")

set(PAIMON_CPP_TOOLCHAIN_ARGS)
set(PAIMON_CPP_SYSTEM_LIBS)
if(PAIMON_ENABLE_OSS OR PAIMON_ENABLE_S3 OR PAIMON_ENABLE_REST)
  find_package(CURL REQUIRED)
  list(APPEND PAIMON_CPP_SYSTEM_LIBS CURL::libcurl)
endif()
if(PAIMON_ENABLE_S3 OR PAIMON_ENABLE_REST)
  find_package(OpenSSL REQUIRED)
  list(APPEND PAIMON_CPP_SYSTEM_LIBS OpenSSL::SSL OpenSSL::Crypto)
endif()
if(CMAKE_TOOLCHAIN_FILE AND DEFINED VCPKG_TARGET_TRIPLET)
  list(APPEND PAIMON_CPP_TOOLCHAIN_ARGS
    -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}
    -DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET}
    -DVCPKG_MANIFEST_MODE=OFF)
  if(DEFINED VCPKG_INSTALLED_DIR)
    list(APPEND PAIMON_CPP_TOOLCHAIN_ARGS
         -DVCPKG_INSTALLED_DIR=${VCPKG_INSTALLED_DIR})
  endif()
endif()

set(PAIMON_CPP_STATIC_DEPENDENCY_LIBS
  ${PAIMON_CPP_BUILD_DIR}/arrow_ep-install/lib/libarrow${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_BUILD_DIR}/arrow_ep-install/lib/libarrow_acero${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_BUILD_DIR}/arrow_ep-install/lib/libarrow_bundled_dependencies${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_BUILD_DIR}/arrow_ep-install/lib/libarrow_dataset${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_BUILD_DIR}/arrow_ep-install/lib/libparquet${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_BUILD_DIR}/arrow_ep-prefix/src/arrow_ep-build/thrift_ep-install/lib/libthrift${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_BUILD_DIR}/avro_ep-install/lib/libavrocpp_s${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_BUILD_DIR}/fmt_ep-install/lib/libfmt${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_BUILD_DIR}/glog_ep-install/lib/libglog${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_BUILD_DIR}/lz4_ep-install/lib/liblz4${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_BUILD_DIR}/orc_ep-prefix/lib/liborc${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_BUILD_DIR}/protobuf_ep-install/lib/libprotobuf${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_BUILD_DIR}/re2_ep-install/lib/libre2${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_BUILD_DIR}/release/libroaring_bitmap${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_BUILD_DIR}/release/libxxhash${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_BUILD_DIR}/snappy_ep-install/lib/libsnappy${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_BUILD_DIR}/tbb_ep-install/lib/libtbb${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_BUILD_DIR}/zlib_ep-install/lib/libz${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_BUILD_DIR}/zstd_ep-install/lib/libzstd${CMAKE_STATIC_LIBRARY_SUFFIX}
)
if(PAIMON_ENABLE_OSS)
  list(APPEND PAIMON_CPP_STATIC_DEPENDENCY_LIBS
       ${PAIMON_CPP_BUILD_DIR}/oss_sdk_v2_ep-install/lib64/libalibabacloud-oss-cpp-sdk-v2${CMAKE_STATIC_LIBRARY_SUFFIX})
endif()
if(PAIMON_ENABLE_S3)
  list(APPEND PAIMON_CPP_STATIC_DEPENDENCY_LIBS
       ${PAIMON_CPP_BUILD_DIR}/aws-auth_ep-install/lib64/libaws-c-auth${CMAKE_STATIC_LIBRARY_SUFFIX}
       ${PAIMON_CPP_BUILD_DIR}/aws-auth_ep-install/lib64/libaws-c-http${CMAKE_STATIC_LIBRARY_SUFFIX}
       ${PAIMON_CPP_BUILD_DIR}/aws-auth_ep-install/lib64/libaws-c-io${CMAKE_STATIC_LIBRARY_SUFFIX}
       ${PAIMON_CPP_BUILD_DIR}/aws-auth_ep-install/lib64/libaws-c-compression${CMAKE_STATIC_LIBRARY_SUFFIX}
       ${PAIMON_CPP_BUILD_DIR}/aws-auth_ep-install/lib64/libaws-c-cal${CMAKE_STATIC_LIBRARY_SUFFIX}
       ${PAIMON_CPP_BUILD_DIR}/aws-auth_ep-install/lib64/libaws-c-sdkutils${CMAKE_STATIC_LIBRARY_SUFFIX}
       ${PAIMON_CPP_BUILD_DIR}/aws-auth_ep-install/lib64/libaws-c-common${CMAKE_STATIC_LIBRARY_SUFFIX})
  if(UNIX AND NOT APPLE)
    list(APPEND PAIMON_CPP_STATIC_DEPENDENCY_LIBS
         ${PAIMON_CPP_BUILD_DIR}/aws-auth_ep-install/lib64/libs2n${CMAKE_STATIC_LIBRARY_SUFFIX})
  endif()
endif()

ExternalProject_Add(paimon_cpp_ep
  SOURCE_DIR ${PAIMON_CPP_SOURCE_DIR}
  BINARY_DIR ${PAIMON_CPP_BUILD_DIR}
  PATCH_COMMAND
    sh -c "${PAIMON_CPP_PATCH_SCRIPT}"
  CMAKE_ARGS
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_INSTALL_PREFIX=${PAIMON_CPP_PREFIX}
    -DCMAKE_INSTALL_LIBDIR=lib64
    -DLIBUNWIND_LIBRARY=FALSE
    -DPAIMON_DEPENDENCY_SOURCE=BUNDLED
    -DPAIMON_BUILD_TESTS=OFF
    -DPAIMON_BUILD_SHARED=OFF
    -DPAIMON_BUILD_STATIC=ON
    -DPAIMON_ENABLE_ORC=ON
    -DPAIMON_ENABLE_AVRO=ON
    -DPAIMON_ENABLE_OSS=${PAIMON_ENABLE_OSS}
    -DPAIMON_ENABLE_S3=${PAIMON_ENABLE_S3}
    -DPAIMON_ENABLE_REST=${PAIMON_ENABLE_REST}
    ${PAIMON_CPP_TOOLCHAIN_ARGS}
  BUILD_BYPRODUCTS
    ${PAIMON_CPP_LIB}/libpaimon${CMAKE_STATIC_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_file_index${CMAKE_STATIC_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_global_index${CMAKE_STATIC_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_local_file_system${CMAKE_STATIC_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_parquet_file_format${CMAKE_STATIC_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_orc_file_format${CMAKE_STATIC_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_avro_file_format${CMAKE_STATIC_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_blob_file_format${CMAKE_STATIC_LIBRARY_SUFFIX}
    $<$<BOOL:${PAIMON_ENABLE_OSS}>:${PAIMON_CPP_LIB}/libpaimon_oss_file_system${CMAKE_STATIC_LIBRARY_SUFFIX}>
    $<$<BOOL:${PAIMON_ENABLE_S3}>:${PAIMON_CPP_LIB}/libpaimon_s3_file_system${CMAKE_STATIC_LIBRARY_SUFFIX}>
    ${PAIMON_CPP_STATIC_DEPENDENCY_LIBS}
)

# Extension sources
set(EXTENSION_SOURCES
  src/paimon_extension.cpp
  src/paimon_functions.cpp
  src/paimon_storage/paimon_scan.cpp
  src/paimon_storage/paimon_snapshots.cpp
  src/paimon_storage/paimon_catalog.cpp
  src/paimon_storage/paimon_insert.cpp
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

# Link with paimon-cpp. The factories in these libraries are registered by
# static initializers, so the archives must be linked as whole archives.
set(PAIMON_CPP_STATIC_LIBS
  ${PAIMON_CPP_LIB}/libpaimon_file_index${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_global_index${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_local_file_system${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_parquet_file_format${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_orc_file_format${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_avro_file_format${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_blob_file_format${CMAKE_STATIC_LIBRARY_SUFFIX}
)
if(PAIMON_ENABLE_OSS)
  list(APPEND PAIMON_CPP_STATIC_LIBS
       ${PAIMON_CPP_LIB}/libpaimon_oss_file_system${CMAKE_STATIC_LIBRARY_SUFFIX})
endif()
if(PAIMON_ENABLE_S3)
  list(APPEND PAIMON_CPP_STATIC_LIBS
       ${PAIMON_CPP_LIB}/libpaimon_s3_file_system${CMAKE_STATIC_LIBRARY_SUFFIX})
endif()
set(PAIMON_CPP_CORE_STATIC_LIB
  ${PAIMON_CPP_LIB}/libpaimon${CMAKE_STATIC_LIBRARY_SUFFIX})
set(PAIMON_CPP_LIBS)
if(APPLE)
  foreach(lib IN LISTS PAIMON_CPP_STATIC_LIBS)
    list(APPEND PAIMON_CPP_LIBS "-Wl,-force_load,${lib}")
  endforeach()
  list(APPEND PAIMON_CPP_LIBS ${PAIMON_CPP_CORE_STATIC_LIB}
       ${PAIMON_CPP_STATIC_DEPENDENCY_LIBS})
else()
  list(APPEND PAIMON_CPP_LIBS -Wl,--whole-archive ${PAIMON_CPP_STATIC_LIBS}
       -Wl,--no-whole-archive -Wl,--start-group
       ${PAIMON_CPP_CORE_STATIC_LIB} ${PAIMON_CPP_STATIC_DEPENDENCY_LIBS}
       -Wl,--end-group)
endif()
list(APPEND PAIMON_CPP_LIBS ${PAIMON_CPP_SYSTEM_LIBS})
target_link_libraries(${EXTENSION_NAME} ${PAIMON_CPP_LIBS})
target_link_libraries(${TARGET_NAME}_loadable_extension ${PAIMON_CPP_LIBS})
if(APPLE)
  target_link_options(${EXTENSION_NAME} PUBLIC "SHELL:-framework Security"
                      "SHELL:-framework CoreFoundation" "SHELL:-framework Network")
  target_link_options(${TARGET_NAME}_loadable_extension PRIVATE "SHELL:-framework Security"
                      "SHELL:-framework CoreFoundation" "SHELL:-framework Network")
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
