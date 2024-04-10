# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

if(NOT DEFINED BIGQUERY_INSTALL_PREFIX OR "${BIGQUERY_INSTALL_PREFIX}" STREQUAL "")
  set(BIGQUERY_INSTALL_PREFIX "${CMAKE_CURRENT_SOURCE_DIR}/install")
endif()

if(NOT DEFINED CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
endif()

# == Generated checksum ==
set(BIGQUERY_VERSION_WITH_HASH "2.22.0")
set(BIGQUERY_HASH_X86_64_APPLE_DARWIN "83f1e647acb6f78f130652ad3af3680cd3d80af413cd1f654f563effd4b7540a")
set(BIGQUERY_HASH_AARCH64_APPLE_DARWIN "e823fbc0ea5fa71d13b921083e697ddf96ead29a70142eef2b51f3d2c454d122")
set(BIGQUERY_HASH_X86_64_LINUX_GNU "500e6880f1eb1bdd1f93c3cf727d55b42aa7d8373873e3bbdbdcd67457179318")
set(BIGQUERY_HASH_AARCH64_LINUX_GNU "21d92d3e0dac58791fcc533289155ccae4cc0ff23e6bf633447f77247bd9b221")
set(BIGQUERY_HASH_X86_64_WINDOWS_MSVC "6651151d6b06865fa701aeef7eaf8d8b89730e3853ce8ee02c771b697c020b80")
# == Generated checksum ==

if(NOT DEFINED BIGQUERY_VERSION OR "${BIGQUERY_VERSION}" STREQUAL "")
  set(BIGQUERY_VERSION "${BIGQUERY_VERSION_WITH_HASH}")
endif()
set(PRECOMPILED_BIGQUERY_BASE_URL "https://github.com/cocoa-xu/google-cloud-cpp-bigquery/releases/download/v${BIGQUERY_VERSION}")

if(UNIX)
  if(APPLE)
    if(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
      set(PRECOMPILED_BIGQUERY_TARBALL_FILENAME "bigquery-${BIGQUERY_VERSION}-x86_64-apple-darwin.tar.gz")
      if(BIGQUERY_VERSION STREQUAL BIGQUERY_VERSION_WITH_HASH)
        set(PRECOMPILED_BIGQUERY_HASH "${BIGQUERY_HASH_X86_64_APPLE_DARWIN}")
      endif()
    elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
      set(PRECOMPILED_BIGQUERY_TARBALL_FILENAME "bigquery-${BIGQUERY_VERSION}-aarch64-apple-darwin.tar.gz")
      if(BIGQUERY_VERSION STREQUAL BIGQUERY_VERSION_WITH_HASH)
        set(PRECOMPILED_BIGQUERY_HASH "${BIGQUERY_HASH_AARCH64_APPLE_DARWIN}")
      endif()
    else()
      message(FATAL_ERROR "Unsupported CPU architecture: ${CMAKE_SYSTEM_PROCESSOR}")
    endif()
  else()
    if(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
      set(PRECOMPILED_BIGQUERY_TARBALL_FILENAME "bigquery-${BIGQUERY_VERSION}-x86_64-linux-gnu.tar.gz")
      if(BIGQUERY_VERSION STREQUAL BIGQUERY_VERSION_WITH_HASH)
        set(PRECOMPILED_BIGQUERY_HASH "${BIGQUERY_HASH_X86_64_LINUX_GNU}")
      endif()
    elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
      set(PRECOMPILED_BIGQUERY_TARBALL_FILENAME "bigquery-${BIGQUERY_VERSION}-aarch64-linux-gnu.tar.gz")
      if(BIGQUERY_VERSION STREQUAL BIGQUERY_VERSION_WITH_HASH)
        set(PRECOMPILED_BIGQUERY_HASH "${BIGQUERY_HASH_AARCH64_LINUX_GNU}")
      endif()
    else()
      message(FATAL_ERROR "Unsupported CPU architecture: ${CMAKE_SYSTEM_PROCESSOR}")
    endif()
  endif()
elseif(WIN32)
  if(CMAKE_SYSTEM_PROCESSOR STREQUAL "AMD64")
    set(PRECOMPILED_BIGQUERY_TARBALL_FILENAME "bigquery-${BIGQUERY_VERSION}-x86_64-windows-msvc.tar.gz")
    if(BIGQUERY_VERSION STREQUAL BIGQUERY_VERSION_WITH_HASH)
      set(PRECOMPILED_BIGQUERY_HASH "${BIGQUERY_HASH_X86_64_WINDOWS_MSVC}")
    endif()
  else()
    message(FATAL_ERROR "Unsupported CPU architecture: ${CMAKE_SYSTEM_PROCESSOR}")
  endif()
else()
  message(FATAL_ERROR "Unsupported operating system: ${CMAKE_SYSTEM_NAME}")
endif()
set(PRECOMPILED_BIGQUERY_URL "${PRECOMPILED_BIGQUERY_BASE_URL}/${PRECOMPILED_BIGQUERY_TARBALL_FILENAME}")

set(CACHE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/cache")
set(PRECOMPILED_BIGQUERY_TARBALL "${CACHE_DIR}/${PRECOMPILED_BIGQUERY_TARBALL_FILENAME}")
if(UNIX)
  set(PRECOMPILED_BIGQUERY_ARCHIVE "${BIGQUERY_INSTALL_PREFIX}/lib/libgoogle_cloud_cpp_bigquery.a")
elseif(WIN32)
  set(PRECOMPILED_BIGQUERY_ARCHIVE "${BIGQUERY_INSTALL_PREFIX}/lib/google_cloud_cpp_bigquery.lib")
else()
  message(FATAL_ERROR "Unsupported operating system: ${CMAKE_SYSTEM_NAME}")
endif()

if(NOT EXISTS "${PRECOMPILED_BIGQUERY_ARCHIVE}")
  if(NOT EXISTS "${PRECOMPILED_BIGQUERY_TARBALL}")
    message(STATUS "Using precompiled bigquery binaries from ${PRECOMPILED_BIGQUERY_URL}")
    if(DEFINED PRECOMPILED_BIGQUERY_HASH)
      file(DOWNLOAD
        "${PRECOMPILED_BIGQUERY_URL}"
        "${PRECOMPILED_BIGQUERY_TARBALL}"
        INACTIVITY_TIMEOUT 300
        SHOW_PROGRESS
        EXPECTED_HASH SHA256=${PRECOMPILED_BIGQUERY_HASH}
      )
    else()
      file(DOWNLOAD
        "${PRECOMPILED_BIGQUERY_URL}"
        "${PRECOMPILED_BIGQUERY_TARBALL}"
        INACTIVITY_TIMEOUT 300
        SHOW_PROGRESS
      )
    endif()
  endif()

  if(UNIX)
    execute_process(
      COMMAND bash -c "mkdir -p \"${BIGQUERY_INSTALL_PREFIX}\" && tar xzf \"${PRECOMPILED_BIGQUERY_TARBALL}\" -C \"${BIGQUERY_INSTALL_PREFIX}\""
      RESULT_VARIABLE STATUS
    )
  elseif(WIN32)
    execute_process(
      COMMAND powershell -command "(New-Item -ItemType Directory -Path \"${BIGQUERY_INSTALL_PREFIX}\" -Force -ErrorAction SilentlyContinue) -and (tar xzf \"${PRECOMPILED_BIGQUERY_TARBALL}\" -C \"${BIGQUERY_INSTALL_PREFIX}\")"
      RESULT_VARIABLE STATUS
    )
  else()
    message(FATAL_ERROR "Unsupported operating system: ${CMAKE_SYSTEM_NAME}")
  endif()
  if(STATUS STREQUAL "0")
    message(STATUS "Precompiled bigquery binaries extracted to ${BIGQUERY_INSTALL_PREFIX}")
  else()
    message(FATAL_ERROR "Failed to extract bigquery binaries from ${PRECOMPILED_BIGQUERY_TARBALL} to ${BIGQUERY_INSTALL_PREFIX}")
  endif()
endif()

set(CMAKE_PREFIX_PATH "${BIGQUERY_INSTALL_PREFIX};${BIGQUERY_INSTALL_PREFIX}/openssl;${CMAKE_PREFIX_PATH}")
set(BIGQUERY_INCLUDE_DIRS "${BIGQUERY_INSTALL_PREFIX}/include")

find_package(OpenSSL REQUIRED)
find_package(Arrow REQUIRED)
find_package(google_cloud_cpp_bigquery REQUIRED PATHS "${BIGQUERY_INSTALL_PREFIX}")
find_package(google_cloud_cpp_rest_internal REQUIRED PATHS "${BIGQUERY_INSTALL_PREFIX}")
find_package(google_cloud_cpp_common REQUIRED PATHS "${BIGQUERY_INSTALL_PREFIX}")

add_library(
    google_cloud_cpp_bigquery_rest # cmake-format: sort
    google/cloud/bigquery/v2/minimal/internal/bigquery_http_response.cc
    google/cloud/bigquery/v2/minimal/internal/bigquery_http_response.h
    google/cloud/bigquery/v2/minimal/internal/common_options.h
    google/cloud/bigquery/v2/minimal/internal/common_v2_resources.cc
    google/cloud/bigquery/v2/minimal/internal/common_v2_resources.h
    google/cloud/bigquery/v2/minimal/internal/dataset.cc
    google/cloud/bigquery/v2/minimal/internal/dataset.h
    google/cloud/bigquery/v2/minimal/internal/dataset_client.cc
    google/cloud/bigquery/v2/minimal/internal/dataset_client.h
    google/cloud/bigquery/v2/minimal/internal/dataset_connection.cc
    google/cloud/bigquery/v2/minimal/internal/dataset_connection.h
    google/cloud/bigquery/v2/minimal/internal/dataset_idempotency_policy.cc
    google/cloud/bigquery/v2/minimal/internal/dataset_idempotency_policy.h
    google/cloud/bigquery/v2/minimal/internal/dataset_logging.cc
    google/cloud/bigquery/v2/minimal/internal/dataset_logging.h
    google/cloud/bigquery/v2/minimal/internal/dataset_metadata.cc
    google/cloud/bigquery/v2/minimal/internal/dataset_metadata.h
    google/cloud/bigquery/v2/minimal/internal/dataset_options.cc
    google/cloud/bigquery/v2/minimal/internal/dataset_options.h
    google/cloud/bigquery/v2/minimal/internal/dataset_request.cc
    google/cloud/bigquery/v2/minimal/internal/dataset_request.h
    google/cloud/bigquery/v2/minimal/internal/dataset_response.cc
    google/cloud/bigquery/v2/minimal/internal/dataset_response.h
    google/cloud/bigquery/v2/minimal/internal/dataset_rest_connection_impl.cc
    google/cloud/bigquery/v2/minimal/internal/dataset_rest_connection_impl.h
    google/cloud/bigquery/v2/minimal/internal/dataset_rest_stub.cc
    google/cloud/bigquery/v2/minimal/internal/dataset_rest_stub.h
    google/cloud/bigquery/v2/minimal/internal/dataset_rest_stub_factory.cc
    google/cloud/bigquery/v2/minimal/internal/dataset_rest_stub_factory.h
    google/cloud/bigquery/v2/minimal/internal/dataset_retry_policy.h
    google/cloud/bigquery/v2/minimal/internal/job.cc
    google/cloud/bigquery/v2/minimal/internal/job.h
    google/cloud/bigquery/v2/minimal/internal/job_client.cc
    google/cloud/bigquery/v2/minimal/internal/job_client.h
    google/cloud/bigquery/v2/minimal/internal/job_configuration.cc
    google/cloud/bigquery/v2/minimal/internal/job_configuration.h
    google/cloud/bigquery/v2/minimal/internal/job_configuration_query.cc
    google/cloud/bigquery/v2/minimal/internal/job_configuration_query.h
    google/cloud/bigquery/v2/minimal/internal/job_connection.cc
    google/cloud/bigquery/v2/minimal/internal/job_connection.h
    google/cloud/bigquery/v2/minimal/internal/job_idempotency_policy.cc
    google/cloud/bigquery/v2/minimal/internal/job_idempotency_policy.h
    google/cloud/bigquery/v2/minimal/internal/job_logging.cc
    google/cloud/bigquery/v2/minimal/internal/job_logging.h
    google/cloud/bigquery/v2/minimal/internal/job_metadata.cc
    google/cloud/bigquery/v2/minimal/internal/job_metadata.h
    google/cloud/bigquery/v2/minimal/internal/job_options.cc
    google/cloud/bigquery/v2/minimal/internal/job_options.h
    google/cloud/bigquery/v2/minimal/internal/job_query_stats.cc
    google/cloud/bigquery/v2/minimal/internal/job_query_stats.h
    google/cloud/bigquery/v2/minimal/internal/job_request.cc
    google/cloud/bigquery/v2/minimal/internal/job_request.h
    google/cloud/bigquery/v2/minimal/internal/job_response.cc
    google/cloud/bigquery/v2/minimal/internal/job_response.h
    google/cloud/bigquery/v2/minimal/internal/job_rest_connection_impl.cc
    google/cloud/bigquery/v2/minimal/internal/job_rest_connection_impl.h
    google/cloud/bigquery/v2/minimal/internal/job_rest_stub.cc
    google/cloud/bigquery/v2/minimal/internal/job_rest_stub.h
    google/cloud/bigquery/v2/minimal/internal/job_rest_stub_factory.cc
    google/cloud/bigquery/v2/minimal/internal/job_rest_stub_factory.h
    google/cloud/bigquery/v2/minimal/internal/job_retry_policy.h
    google/cloud/bigquery/v2/minimal/internal/job_stats.cc
    google/cloud/bigquery/v2/minimal/internal/job_stats.h
    google/cloud/bigquery/v2/minimal/internal/json_utils.cc
    google/cloud/bigquery/v2/minimal/internal/json_utils.h
    google/cloud/bigquery/v2/minimal/internal/log_wrapper.h
    google/cloud/bigquery/v2/minimal/internal/project.cc
    google/cloud/bigquery/v2/minimal/internal/project.h
    google/cloud/bigquery/v2/minimal/internal/project_client.cc
    google/cloud/bigquery/v2/minimal/internal/project_client.h
    google/cloud/bigquery/v2/minimal/internal/project_connection.cc
    google/cloud/bigquery/v2/minimal/internal/project_connection.h
    google/cloud/bigquery/v2/minimal/internal/project_idempotency_policy.cc
    google/cloud/bigquery/v2/minimal/internal/project_idempotency_policy.h
    google/cloud/bigquery/v2/minimal/internal/project_logging.cc
    google/cloud/bigquery/v2/minimal/internal/project_logging.h
    google/cloud/bigquery/v2/minimal/internal/project_metadata.cc
    google/cloud/bigquery/v2/minimal/internal/project_metadata.h
    google/cloud/bigquery/v2/minimal/internal/project_options.cc
    google/cloud/bigquery/v2/minimal/internal/project_options.h
    google/cloud/bigquery/v2/minimal/internal/project_request.cc
    google/cloud/bigquery/v2/minimal/internal/project_request.h
    google/cloud/bigquery/v2/minimal/internal/project_response.cc
    google/cloud/bigquery/v2/minimal/internal/project_response.h
    google/cloud/bigquery/v2/minimal/internal/project_rest_connection_impl.cc
    google/cloud/bigquery/v2/minimal/internal/project_rest_connection_impl.h
    google/cloud/bigquery/v2/minimal/internal/project_rest_stub.cc
    google/cloud/bigquery/v2/minimal/internal/project_rest_stub.h
    google/cloud/bigquery/v2/minimal/internal/project_rest_stub_factory.cc
    google/cloud/bigquery/v2/minimal/internal/project_rest_stub_factory.h
    google/cloud/bigquery/v2/minimal/internal/project_retry_policy.h
    google/cloud/bigquery/v2/minimal/internal/rest_stub_utils.cc
    google/cloud/bigquery/v2/minimal/internal/rest_stub_utils.h
    google/cloud/bigquery/v2/minimal/internal/table.cc
    google/cloud/bigquery/v2/minimal/internal/table.h
    google/cloud/bigquery/v2/minimal/internal/table_client.cc
    google/cloud/bigquery/v2/minimal/internal/table_client.h
    google/cloud/bigquery/v2/minimal/internal/table_connection.cc
    google/cloud/bigquery/v2/minimal/internal/table_connection.h
    google/cloud/bigquery/v2/minimal/internal/table_constraints.cc
    google/cloud/bigquery/v2/minimal/internal/table_constraints.h
    google/cloud/bigquery/v2/minimal/internal/table_idempotency_policy.cc
    google/cloud/bigquery/v2/minimal/internal/table_idempotency_policy.h
    google/cloud/bigquery/v2/minimal/internal/table_logging.cc
    google/cloud/bigquery/v2/minimal/internal/table_logging.h
    google/cloud/bigquery/v2/minimal/internal/table_metadata.cc
    google/cloud/bigquery/v2/minimal/internal/table_metadata.h
    google/cloud/bigquery/v2/minimal/internal/table_options.cc
    google/cloud/bigquery/v2/minimal/internal/table_options.h
    google/cloud/bigquery/v2/minimal/internal/table_partition.cc
    google/cloud/bigquery/v2/minimal/internal/table_partition.h
    google/cloud/bigquery/v2/minimal/internal/table_request.cc
    google/cloud/bigquery/v2/minimal/internal/table_request.h
    google/cloud/bigquery/v2/minimal/internal/table_response.cc
    google/cloud/bigquery/v2/minimal/internal/table_response.h
    google/cloud/bigquery/v2/minimal/internal/table_rest_connection_impl.cc
    google/cloud/bigquery/v2/minimal/internal/table_rest_connection_impl.h
    google/cloud/bigquery/v2/minimal/internal/table_rest_stub.cc
    google/cloud/bigquery/v2/minimal/internal/table_rest_stub.h
    google/cloud/bigquery/v2/minimal/internal/table_rest_stub_factory.cc
    google/cloud/bigquery/v2/minimal/internal/table_rest_stub_factory.h
    google/cloud/bigquery/v2/minimal/internal/table_retry_policy.h
    google/cloud/bigquery/v2/minimal/internal/table_schema.cc
    google/cloud/bigquery/v2/minimal/internal/table_schema.h
    google/cloud/bigquery/v2/minimal/internal/table_view.cc
    google/cloud/bigquery/v2/minimal/internal/table_view.h)
target_include_directories(
    google_cloud_cpp_bigquery_rest
    PUBLIC ${CMAKE_CURRENT_LIST_DIR}
    PUBLIC ${BIGQUERY_INSTALL_PREFIX})
target_link_libraries(
    google_cloud_cpp_bigquery_rest PUBLIC google-cloud-cpp::rest_internal
                                          google-cloud-cpp::common)

set(BIGQUERY_LINK_LIBRARIES google-cloud-cpp::bigquery Arrow::arrow_static google_cloud_cpp_bigquery_rest)

if(NOT DEFINED ADBC_DRIVER_BIGQUERY_ENDIAN)
  if(CMAKE_VERSION VERSION_LESS "3.20.0")
    INCLUDE(TestBigEndian)
    TEST_BIG_ENDIAN(ADBC_DRIVER_BIGQUERY_ENDIAN_TEST)
    if(ADBC_DRIVER_BIGQUERY_ENDIAN_TEST)
      set(ADBC_DRIVER_BIGQUERY_ENDIAN BIG_ENDIAN)
    else()
      set(ADBC_DRIVER_BIGQUERY_ENDIAN LITTLE_ENDIAN)
    endif()
  else()
    if(DEFINED CMAKE_CXX_BYTE_ORDER AND CMAKE_CXX_BYTE_ORDER STREQUAL "BIG_ENDIAN")
      set(ADBC_DRIVER_BIGQUERY_ENDIAN BIG_ENDIAN)
    else()
      set(ADBC_DRIVER_BIGQUERY_ENDIAN LITTLE_ENDIAN)
    endif()
  endif()
endif()
message(STATUS "ADBC_DRIVER_BIGQUERY_ENDIAN: ${ADBC_DRIVER_BIGQUERY_ENDIAN}")
if(ADBC_DRIVER_BIGQUERY_ENDIAN STREQUAL "BIG_ENDIAN")
  set(ADBC_DRIVER_BIGQUERY_ENDIAN_DEF "ADBC_DRIVER_BIGQUERY_ENDIAN=0")
else()
  set(ADBC_DRIVER_BIGQUERY_ENDIAN_DEF "ADBC_DRIVER_BIGQUERY_ENDIAN=1")
endif()

add_arrow_lib(adbc_driver_bigquery
              SOURCES
              bigquery.cc
              connection.cc
              database.cc
              statement.cc
              readrowsiterator.cc
              OUTPUTS
              ADBC_LIBRARIES
              CMAKE_PACKAGE_NAME
              AdbcDriverBigQuery
              PKG_CONFIG_NAME
              adbc-driver-bigquery
              SHARED_LINK_FLAGS
              ${ADBC_LINK_FLAGS}
              SHARED_LINK_LIBS
              ${BIGQUERY_LINK_LIBRARIES}
              adbc_driver_common
              nanoarrow
              STATIC_LINK_LIBS
              ${BIGQUERY_LINK_LIBRARIES}
              adbc_driver_common
              nanoarrow
              ${BIGQUERY_LINK_LIBRARIES})

foreach(LIB_TARGET ${ADBC_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ADBC_EXPORTING "${ADBC_DRIVER_BIGQUERY_ENDIAN_DEF}")

  target_include_directories(${LIB_TARGET} SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}
                                     ${REPOSITORY_ROOT}/c/
                                     ${BIGQUERY_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/vendor
                                     ${REPOSITORY_ROOT}/c/driver)
endforeach()

if(ADBC_TEST_LINKAGE STREQUAL "shared")
  set(TEST_LINK_LIBS adbc_driver_bigquery_shared)
else()
  set(TEST_LINK_LIBS adbc_driver_bigquery_static)
endif()

if(ADBC_BUILD_TESTS)
  add_test_case(driver_bigquery_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-bigquery
                SOURCES
                bigquery_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                adbc_validation
                nanoarrow
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-bigquery-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-bigquery-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}
                                     ${REPOSITORY_ROOT}/c/
                                     ${BIGQUERY_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/vendor
                                     ${REPOSITORY_ROOT}/c/driver)
  adbc_configure_target(adbc-driver-bigquery-test)
endif()
