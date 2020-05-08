# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

set(_api_proto triton.proto)
get_filename_component(_api_proto_abs ${_api_proto} ABSOLUTE)
get_filename_component(_api_proto_dir ${_api_proto_abs} DIRECTORY)

get_target_property(_grpc_source grpc++ SOURCE_DIR)
set(_googleapis_include ${_grpc_source}/third_party/googleapis)

get_target_property(_protobuf_include_dirs protobuf::libprotoc INCLUDE_DIRECTORIES)

# Generated sources
set(_api_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/triton.pb.cc")
set(_api_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/triton.pb.h")
set(_api_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/triton.grpc.pb.cc")
set(_api_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/triton.grpc.pb.h")

set(_api_go_srcs "${CMAKE_CURRENT_BINARY_DIR}/triton.pb.go")
set(_api_go_rest_srcs "${CMAKE_CURRENT_BINARY_DIR}/triton.pb.gw.go")

set(_api_swagger_json "${CMAKE_CURRENT_BINARY_DIR}/triton.swagger.json")

# C++ gRPC files
add_custom_command(
  OUTPUT "${_api_proto_srcs}" "${_api_proto_hdrs}" "${_api_grpc_srcs}"
         "${_api_grpc_hdrs}"
  COMMAND
    $<TARGET_FILE:protoc> --grpc_out ${CMAKE_CURRENT_BINARY_DIR} --cpp_out
    ${CMAKE_CURRENT_BINARY_DIR} -I ${_api_proto_dir} -I ${_googleapis_include}
    "-I$<JOIN:${_protobuf_include_dirs},;-I>"
    --plugin=protoc-gen-grpc=$<TARGET_FILE:grpc_cpp_plugin> ${_api_proto}
  MAIN_DEPENDENCY "${_api_proto}"
  COMMENT "Running C++ protocol buffer compiler on ${_api_proto}"
  VERBATIM
  COMMAND_EXPAND_LISTS
)

set(_annotations_pb_cc "${CMAKE_CURRENT_BINARY_DIR}/google/api/annotations.pb.cc")
set(_annotations_pb_h "${CMAKE_CURRENT_BINARY_DIR}/google/api/annotations.pb.h")
set(_annotations_proto "${_googleapis_include}/google/api/annotations.proto")
set(_http_pb_cc "${CMAKE_CURRENT_BINARY_DIR}/google/api/http.pb.cc")
set(_http_pb_h "${CMAKE_CURRENT_BINARY_DIR}/google/api/http.pb.h")
set(_http_proto "${_googleapis_include}/google/api/http.proto")

# Compile googleapis/annotations
add_custom_command(
  OUTPUT "${_annotations_pb_cc}" "${_annotations_pb_h}"
  "${_http_pb_cc}" "${_http_pb_h}"
  COMMAND
    $<TARGET_FILE:protoc> --cpp_out ${CMAKE_CURRENT_BINARY_DIR}
    -I ${_googleapis_include}
    "-I$<JOIN:${_protobuf_include_dirs},;-I>"
    ${_annotations_proto} ${_http_proto}
  DEPENDS ${_annotations_proto} ${_http_proto}
  COMMENT "Running C++ protocol buffer compiler on googleapi dependencies"
  VERBATIM
  COMMAND_EXPAND_LISTS
)

add_library(api STATIC ${_api_proto_srcs} ${_annotations_pb_cc} ${_http_pb_cc})
include_directories(api ${CMAKE_CURRENT_BINARY_DIR})
include_directories(api ${_protobuf_include_dirs})

target_link_libraries(api ${PROTOBUF_LIBRARY})
target_include_directories(api PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(api PUBLIC ${_protobuf_include_dirs})

add_library(api-grpc STATIC ${_api_grpc_srcs})
target_link_libraries(api-grpc ${PROTOBUF_LIBRARY})
target_link_libraries(api-grpc grpc++)
target_include_directories(api-grpc PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(api-grpc PUBLIC ${googleapis_BINARY_DIR})

# Go gRPC and REST files We are just generating source files and not adding
# libraries

add_custom_command(
  OUTPUT "${_api_go_srcs}" "${_api_go_rest_srcs}"
  COMMAND
    $<TARGET_FILE:protoc>
    --go_out=plugins=grpc:${CMAKE_CURRENT_BINARY_DIR}
    -I ${_api_proto_dir} -I ${_googleapis_include}
    "-I$<JOIN:${_protobuf_include_dirs},;-I>"
    --grpc-gateway_out=logtostderr=true:${CMAKE_CURRENT_BINARY_DIR}
    ${_api_proto_abs}
  MAIN_DEPENDENCY "${_api_proto_abs}"
  COMMENT "Running go protocol buffer compiler on ${_api_proto}"
  VERBATIM
  COMMAND_EXPAND_LISTS
)

add_custom_target(api-proto-go ALL DEPENDS ${_api_go_srcs})

# Swagger JSON files

add_custom_command(
  OUTPUT "${_api_swagger_json}"
  COMMAND
    $<TARGET_FILE:protoc>
    -I ${_api_proto_dir} -I ${_googleapis_include}
    "-I$<JOIN:${_protobuf_include_dirs},;-I>"
    --swagger_out=logtostderr=true:${CMAKE_CURRENT_BINARY_DIR}
    ${_api_proto_abs}
  MAIN_DEPENDENCY "${_api_proto_abs}"
  COMMENT "Generating Swagger JSON files for ${_api_proto}"
  VERBATIM
  COMMAND_EXPAND_LISTS
)

add_custom_target(api-swagger ALL DEPENDS ${_api_swagger_json})
