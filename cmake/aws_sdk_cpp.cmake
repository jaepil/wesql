# Copyright (c) 2024, ApeCloud Inc Holding Limited.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have included with MySQL.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

# cmake -DWITH_OBJSTORE=system|bundled
# bundled is the default

SET(OBJSTORE_INCLUDE_DIR "system default header")
SET(OBJSTORE_LIBRARY_PATH "system default lib path")
SET(OBJSTORE_LIBRARIES "aws-cpp-sdk-s3;aws-cpp-sdk-core")
SET(OBJSTORE_PLATFORM_DEPS "pthread;curl")

MACRO(SHOW_OBJSTORE_INFO)
  MESSAGE(STATUS "OBJSTORE_INCLUDE_DIR: ${OBJSTORE_INCLUDE_DIR}")
  MESSAGE(STATUS "OBJSTORE_LIBRARY_PATH: ${OBJSTORE_LIBRARY_PATH}")
  MESSAGE(STATUS "OBJSTORE_LIBRARIES: ${OBJSTORE_LIBRARIES}")
  MESSAGE(STATUS "OBJSTORE_PLATFORM_DEPS: ${OBJSTORE_PLATFORM_DEPS}")
ENDMACRO()

MACRO(PREPARE_BUNDLED_OJBSTORE)
  SET(OBJSTORE_INSTALL_PREFIX "${CMAKE_BINARY_DIR}/extra/aws-sdk-cpp")

  INCLUDE(ExternalProject)
  ExternalProject_Add(
    aws-sdk-cpp-ext-proj
    SOURCE_DIR "${PROJECT_SOURCE_DIR}/extra/aws-sdk-cpp"
    GIT_REPOSITORY "https://github.com/aws/aws-sdk-cpp.git"
    GIT_TAG "1.11.283"
    UPDATE_COMMAND "" #git submodule update --init --recursive
    # TODO: build with static lib
    CMAKE_ARGS
      -DCMAKE_BUILD_TYPE=Release
      -DBUILD_ONLY=s3
      -DBUILD_SHARED_LIBS=ON
      -DCMAKE_INSTALL_PREFIX=${OBJSTORE_INSTALL_PREFIX}
      -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
      -DENABLE_TESTING=OFF
      -DAUTORUN_UNIT_TESTS=OFF
    BUILD_ALWAYS      TRUE
    TEST_COMMAND      ""
  )

  # compilation result installation is later phase, mkdir include path in advance avoid compile error.
  FILE(MAKE_DIRECTORY "${OBJSTORE_INSTALL_PREFIX}/include")
ENDMACRO()

MACRO(FIND_SYSTEM_OBJSTORE)
  FIND_PACKAGE(AWSSDK REQUIRED COMPONENTS "s3")
ENDMACRO()

MACRO (MYSQL_CHECK_OBJSTORE)
  IF(NOT WITH_OBJSTOR)
    SET(WITH_OBJSTOR "bundled" CACHE STRING "By default use bundled ObjStore library")
  ENDIF()

  IF(WITH_OBJSTOR STREQUAL "bundled")
    MESSAGE(STATUS "WITH_OBJSTOR is bundled, download aws-sdk-cpp and compile it")
    PREPARE_BUNDLED_OJBSTORE()
    MESSAGE(STATUS "aws-sdk-cpp will be installed to ${OBJSTORE_INSTALL_PREFIX} in the compile phase")

    # Set the variables for the project
    SET(OBJSTORE_INCLUDE_DIR "${OBJSTORE_INSTALL_PREFIX}/include")
    SET(OBJSTORE_LIBRARY_PATH "${OBJSTORE_INSTALL_PREFIX}/lib64")
    SET(OBJSTORE_LIBRARY "aws-cpp-sdk-s3;aws-cpp-sdk-core")
    SET(OBJSTORE_PLATFORM_DEPS "pthread;curl")
    # Prepare include and ld path
    INCLUDE_DIRECTORIES(${OBJSTORE_INCLUDE_DIR})
    LINK_DIRECTORIES(${OBJSTORE_LIBRARY_PATH})
  ELSEIF(WITH_OBJSTOR STREQUAL "system")
    MESSAGE(STATUS "WITH_OBJSTOR is system, use system aws s3 lib")
    FIND_SYSTEM_OBJSTORE()
    # avoid error when add_dependencies(aws-sdk-cpp-ext-proj) in the main project
    ADD_CUSTOM_TARGET(aws-sdk-cpp-ext-proj COMMAND "")
    SET(OBJSTORE_LIBRARY ${AWSSDK_LINK_LIBRARIES})
    SET(OBJSTORE_PLATFORM_DEPS ${OBJSTORE_PLATFORM_DEPS})
  ELSE()
    MESSAGE(FATAL_ERROR "WITH_OBJSTOR must be bundled or system")
  ENDIF()

  SHOW_OBJSTORE_INFO()
ENDMACRO()
