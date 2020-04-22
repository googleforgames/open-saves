# Development Guide

Triton is a service that platform agnostic cloud storage & save solution that
runs within [Kubernetes](https://kubernetes.io).

## Install Prerequisites

To build Triton you'll need the following applications installed.

- [Git](https://git-scm.com/downloads)
- [Go](https://golang.org/doc/install)
- [Docker](https://docs.docker.com/install/) including the
  [post-install steps](https://docs.docker.com/install/linux/linux-postinstall/).
- [Bazel](https://docs.bazel.build/versions/master/install.html)
  - You can also use [Bazelisk](https://github.com/bazelbuild/bazelisk) to manage Bazel installations.
  - Windows: Follow the instructions of [Installing Bazel on Windows](https://docs.bazel.build/versions/master/install-windows.html) and [Using rules_go on Windows](https://github.com/bazelbuild/rules_go/blob/master/windows.rst) (installing msys2 and Visual Studio, setting appropriate envrionment variables, etc).
- A working C/C++ toolchain
  - Windows, we use [Visual Studio 2019](https://visualstudio.microsoft.com/vs/) for development

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

To build everything, simply run

```bash
bazel build ...
```

from command line. Bazel will download necessary toolchains and libraries and build binaries. Ouput binaries will be placed under the `bazel-bin/` directory. Please reference [Output directory layout](https://docs.bazel.build/versions/master/output_directories.html) of the Bazel document to learn more about the directory structure.

To build a single target, run `bazel build //<module>:<target>`.

### Updating Go build dependencies

Go dependencies in Bazel are managed by [Gazelle](https://github.com/bazelbuild/bazel-gazelle). If you have changes in dependencies (i.e. adding a new source file), run

```bash
bazel run //:gazelle update
```

in the workspace root directory to update Bazel BUILD files.

### Updating Go modules

We use Go Modules to manager external dependencies. In order to reflect changes in the `go.mod` file to Bazel BUILD files, run

```bash
bazel run //:gazelle -- update-repos -from_file=go.mod
```

in the workspace root directory.

### Running simple gRPC server/client

To stand up the gRPC server, there's a lightweight wrapper around the server code that lives in `cmd/main.go`. To start this, run

```bash
bazel run //cmd:cmd
```

You should see an output like the following

```
bazel run cmd:cmd
INFO: Analyzed target //cmd:cmd (5 packages loaded, 251 targets configured).
INFO: Found 1 target...
Target //cmd:cmd up-to-date:
  bazel-bin/cmd/darwin_amd64_stripped/cmd
INFO: Elapsed time: 2.076s, Critical Path: 0.81s
INFO: 1 process: 1 darwin-sandbox.
INFO: Build completed successfully, 3 total actions
INFO: Build completed successfully, 3 total actions
INFO[0000] starting server on tcp :6000
```

To test the server is actually running, there is a sample gRPC client usage in `examples/grpc-client/main.go`. While the server is running, run

```bash
bazel run //examples/grpc-client:grpc-client
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
