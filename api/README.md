# Open Saves API

This directory contains both the proto and generated Go code for running
Open Saves.

`open_saves.proto` contains the source definition.

`open_saves.pb.go` is generated code containing message definitions.

`open_saves_grpc.pb.go` is generated code containing service definitions.

## Proto documentation

To generate the [reference](../docs/reference.md) page from `open_saves.proto`, do
the following:

1. Download the latest release of [Protocol Buffers](https://github.com/protocolbuffers/protobuf/releases/).
2. Place the `protoc` binary somewhere in your PATH.
3. Install the latest release of [protoc-gen-doc](https://github.com/pseudomuto/protoc-gen-doc).
4. Run the following command:

   ```
   protoc \
      --proto_path=<path-to-protoc-include-dir> \
      --proto_path=<path-to-proto-dir> \
      --doc_out=<path-to-output-docs-folder> \
      --doc_opt=markdown,reference.md \
      <path-to-proto-file>
   ```
   
   The following is an example command:

   ```
   protoc \
      --proto_path=$HOME/projects/protoc-3.17.3-osx-x86_64/include/ \
      --proto_path=$HOME/projects/open-saves/api/ \
      --doc_out=$HOME/projects/open-saves/docs/ \
      --doc_opt=markdown,reference.md \
      $HOME/projects/open-saves/api/*.proto
   ```
 

