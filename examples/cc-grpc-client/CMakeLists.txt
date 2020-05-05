find_package(Protobuf REQUIRED)
# include_directories(${Protobuf_INCLUDE_DIRS})
# include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_executable(cc-grpc-client main.cc)
target_link_libraries(cc-grpc-client api googleapis-c++::api_annotations_protos)

