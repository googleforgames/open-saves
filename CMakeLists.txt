cmake_minimum_required(VERSION 3.17)
project(Triton)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED true)
set(CMAKE_EXPORT_COMPILE_COMMANDS true)

set(GOPATH $ENV{GOPATH})

find_package(Protobuf REQUIRED)

include(FetchContent)
FetchContent_Declare(
  gRPC
  GIT_REPOSITORY https://github.com/grpc/grpc
  GIT_TAG        v1.28.1
)
FetchContent_MakeAvailable(gRPC)

FetchContent_Declare(
  googleapis
  GIT_REPOSITORY https://github.com/googleapis/cpp-cmakefiles
  GIT_TAG        v0.9.0
)
FetchContent_MakeAvailable(googleapis)

FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest
  GIT_TAG        release-1.10.0
)
FetchContent_MakeAvailable(googletest)

add_subdirectory(api)
add_subdirectory(examples)
