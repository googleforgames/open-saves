# Development Guide

Triton is a service that platform agnostic cloud storage & save solution that
runs within [Kubernetes](https://kubernetes.io).

## Install Prerequisites

To build Triton you'll need the following applications installed.

- [Git](https://git-scm.com/downloads)
- [Go](https://golang.org/doc/install)
  - On Windows, you need to make sure that the environment variable GOBIN is left undefined to work around an issue with building the gazelle tools. Otherwise you are going to get an error when trying to build/run `bazel run //:gazelle update` or `bazel build //:buildifier`
- [Docker](https://docs.docker.com/install/) including the
  [post-install steps](https://docs.docker.com/install/linux/linux-postinstall/).
- A working C/C++ toolchain
  - [CMake](https://cmake.org/) is required to build the C++ code
  - On Windows, we use [Visual Studio 2019](https://visualstudio.microsoft.com/vs/) for development
- [Protobuf](https://developers.google.com/protocol-buffers/docs/downloads)
- [Go support for Protocol Buffers](https://github.com/golang/protobuf)

You can install required go modules to compile protos by running:
```bash
go get -u \
  golang.org/x/lint/golint \
  github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway \
  github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger \
  github.com/golang/protobuf/protoc-gen-go
```

Optional Software

- [Google Cloud Platform](cloud.google.com)
- [VirtualBox](https://www.virtualbox.org/wiki/Downloads) or
  Hyperkit[https://minikube.sigs.k8s.io/docs/reference/drivers/hyperkit/] for
  [Minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/).

_It's recommended that you install Go using their instructions because package
managers tend to lag behind the latest Go releases._

## Get the Code

```bash
# Create a directory for the project.
mkdir -p $HOME/workspace
cd $HOME/workspace
# Download the source code.
git clone https://github.com/googleforgames/triton.git
cd triton
```

_Typically for contributing you'll want to
[create a fork](https://help.github.com/en/articles/fork-a-repo) and use that
but for purpose of this guide we'll be using the upstream/master._

## Building

In order to build the main triton server, simply invoke the Makefile in the root project directory by running

```bash
make
```

from command line. This will compile protos and build binaries. Ouput binaries will be placed under the `build/` directory.

### Updating Go build dependencies

We use [Go modules](https://github.com/golang/go/wiki/Modules) to manage dependencies. If you have changes in dependencies (i.e. adding a new source file), add the package to `go.mod`.

### Running simple gRPC server/client

To stand up the gRPC server, there's a lightweight wrapper around the server code that lives in `cmd/server/main.go`. To start this, run

```bash
./build/server
```

You should see an output like the following

```bash
$ ./build/server
INFO[0000] starting server on tcp :6000
```

To test the server is actually running, there is a sample gRPC client usage in `examples/grpc-client/main.go`. While the server is running, run

```bash
go run examples/grpc-client/main.go
```

## Deploying to Kubernetes

TODO

## IDE Support

Triton is a standard Go project so any IDE that understands that should
work. We use [Go Modules](https://github.com/golang/go/wiki/Modules) which is a
relatively new feature in Go so make sure the IDE you are using was built around
Summer 2019. The latest version of
[Visual Studio Code](https://code.visualstudio.com/download) supports it.

# Build all Docker images

TODO

# Contributing to the project

Check out [How to Contribute](contributing.md) before contributing to the project.
