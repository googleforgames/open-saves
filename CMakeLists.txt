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

# Threads is required for gRPC
find_package(Threads REQUIRED)

include(FetchContent)
set(FETCHCONTENT_QUIET OFF)

# Download the tagged version and use it across the build process
FetchContent_Declare(
  gRPC
  GIT_REPOSITORY https://github.com/grpc/grpc
  GIT_TAG v1.28.1)
FetchContent_MakeAvailable(gRPC)

# Can use googletest for C++ tests
# FetchContent_Declare(
#   googletest
#   GIT_REPOSITORY https://github.com/google/googletest
#   GIT_TAG release-1.10.0)
#
# FetchContent_MakeAvailable(googletest)

add_subdirectory(api)
# Add examples once we have some C++ code there
# add_subdirectory(examples)

# Examples

add_custom_target(
    Examples
    COMMENT "Building examples"
    DEPENDS grpc-client cc-grpc-client
)
