set(api_proto triton.proto)
get_filename_component(api_proto_abs ${api_proto} ABSOLUTE)
get_filename_component(api_proto_dir ${api_proto_abs} DIRECTORY)

# Generated sources
set(api_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/triton.pb.cc")
set(api_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/triton.pb.h")
set(api_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/triton.grpc.pb.cc")
set(api_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/triton.grpc.pb.h")
set(api_proto_go_srcs "${CMAKE_CURRENT_BINARY_DIR}/triton.pb.go")

add_library(api STATIC ${_out_var_api})
target_link_libraries(api ${PROTOBUF_LIBRARY})
target_include_directories(api PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(api PUBLIC ${Protobuf_INCLUDE_DIRS})
target_include_directories(api PUBLIC ${googleapis_BINARY_DIR})
protobuf_generate(
  TARGET
  api
  LANGUAGE
  cpp
  OUT_VAR
  _out_var_api
  IMPORT_DIRS
  ${GOPATH}/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis
  PROTOS
  ${api_proto})

add_library(api_grpc STATIC ${_out_var_api_grpc})
target_link_libraries(api_grpc ${PROTOBUF_LIBRARY})
target_link_libraries(api_grpc grpc++)
target_include_directories(api_grpc PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(api_grpc PUBLIC ${Protobuf_INCLUDE_DIRS})
target_include_directories(api_grpc PUBLIC ${googleapis_BINARY_DIR})

protobuf_generate(
  TARGET
  api_grpc
  LANGUAGE
  cpp
  OUT_VAR
  _out_var_api_grpc
  PLUGIN
  protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
  IMPORT_DIRS
  ${GOPATH}/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis
  PROTOS
  ${api_proto})

ExternalProject_Get_Property(googleapis_download SOURCE_DIR)
set(googleapis_include ${SOURCE_DIR})

add_custom_command(
  OUTPUT ${api_proto_go_srcs}
  COMMAND
    $<TARGET_FILE:protoc> ARGS --go_out=plugins=grpc:${CMAKE_CURRENT_BINARY_DIR}
    -I${api_proto_dir} -I${googleapis_include} -I${Protobuf_INCLUDE_DIRS}
    ${api_proto_abs}
  DEPENDS ${api_proto_abs}
  COMMENT "Running go protocol buffer compiler on ${api_proto}"
  VERBATIM)

add_custom_target(api_proto_go DEPENDS ${api_proto_go_srcs})
