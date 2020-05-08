# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.16)
project(Triton CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED true)
set(CMAKE_EXPORT_COMPILE_COMMANDS true)

set(GOPATH $ENV{GOPATH})

find_package(Threads REQUIRED)

# Try the config mode first (primarily for vcpkg)
# find_package(Protobuf CONFIG)
# if(NOT Protobuf_FOUND)
#   find_package(Protobuf REQUIRED)
# endif()

include(FetchContent)
set(FETCHCONTENT_QUIET OFF)

# add_subdirectory(third_party/protobuf/cmake third_party/protobuf)

# Try using locally installed gRPC. Fetch from github if not found
FetchContent_Declare(
  gRPC
  GIT_REPOSITORY https://github.com/grpc/grpc
  GIT_TAG v1.28.1)
FetchContent_MakeAvailable(gRPC)

# Always download googleapis and googletest from GitHub

# FetchContent_Declare(
#   googleapis
#   GIT_REPOSITORY https://github.com/googleapis/cpp-cmakefiles
#   GIT_TAG v0.9.0)

FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest
  GIT_TAG release-1.10.0)

FetchContent_MakeAvailable(googletest)

add_subdirectory(api)
add_subdirectory(examples)

# Main server binary

add_custom_target(
  server ALL
    go build -o ${CMAKE_CURRENT_BINARY_DIR}/server
    ${CMAKE_CURRENT_SOURCE_DIR}/cmd/server/main.go
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building go server binary"
  DEPENDS api-proto-go
  VERBATIM
)

# Examples

add_custom_target(
    Examples
    COMMENT "Building examples"
    DEPENDS grpc-client cc-grpc-client
)
