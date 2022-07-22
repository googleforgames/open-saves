# Development Guide

Open Saves is a service that platform agnostic cloud storage & save solution that
runs within [Kubernetes](https://kubernetes.io).

## Install Prerequisites

To build Open Saves you'll need the following applications installed.

- [Git](https://git-scm.com/downloads)
- [Go](https://golang.org/doc/install)
- [Docker](https://docs.docker.com/install/) including the
  [post-install steps](https://docs.docker.com/install/linux/linux-postinstall/).
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
  [Hyperkit](https://minikube.sigs.k8s.io/docs/reference/drivers/hyperkit/) for
  [Minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/).

_It's recommended that you install Go using their instructions because package
managers tend to lag behind the latest Go releases._

## Get the Code

```bash
# Create a directory for the project.
mkdir -p $HOME/workspace
cd $HOME/workspace
# Download the source code.
git clone https://github.com/googleforgames/open-saves.git
cd open-saves
```

_Typically for contributing you'll want to
[create a fork](https://help.github.com/en/articles/fork-a-repo) and use that
but for purpose of this guide we'll be using the upstream/main._

## Building

In order to build the main Open Saves server, simply invoke the Makefile in the root project directory by running

```bash
make
```

from command line. This will compile protos and build binaries. Ouput binaries will be placed under the `build/` directory.

### Updating Go build dependencies

We use [Go modules](https://github.com/golang/go/wiki/Modules) to manage dependencies. If you have changes in dependencies (i.e. adding a new source file), add the package to `go.mod`.

## Running tests

In order to run the test, use

```bash
make test
```

### Running a simple gRPC server/client

To stand up the gRPC server, there's a lightweight wrapper around the server code
that lives in `cmd/server/main.go`. To start this, run

```bash
./build/server -cloud=gcp -project="<your GCP project>" \
  -bucket="gs://<your-bucket>" -cache="<redis-server-address>:6379"
```

You should see an output like the following

```bash
$ ./build/server -cloud=gcp -project="your-project" -bucket="gs://your-bucket" -cache="localhost:6379"
INFO[0000] Instantiating Open Saves server on GCP
INFO[0000] starting server on tcp :6000
```

To test the server is actually running, there is a sample gRPC client usage in
`examples/grpc-client/main.go`. While the server is running, run

```bash
go run examples/grpc-client/main.go -address=localhost:6000 -insecure=true
```

### Starting cache store

If using Redis for your cache store, you will need to pass in a path to Redis
instance when starting the cache store. As an example, if in a development
environment, you can run the following command.

```bash
./build/server -cloud=gcp -bucket="gs://your-bucket" -cache="localhost:6379"
```

If using [Memorystore](https://cloud.google.com/memorystore) for Redis, you will
get a private IP. In this case, to test locally, you would need to use port
forwarding from a Compute Engine instance. The following commands illustrates
how to create a new Compute Engine instance and forward port 6379 for the Redis
host at `10.0.0.3` to `localhost:6379`.

```bash
gcloud compute instances create redis-forwarder --machine-type=f1-micro
gcloud compute ssh redis-forwarder -- -N -L 6379:10.0.0.3:6379
```

## Setting up Google Cloud

You need to set up [Cloud Firestore in Datastore mode](https://cloud.google.com/datastore/docs)
(Datastore) and [Cloud Storage](https://cloud.google.com/storage) to run the
current version of Open Saves.

### Cloud Firestore in Datastore mode

Cloud Firestore in Datastore mode (Datastore) is primarily used to manage
metadata of Open Saves. Smaller blob data (usually up to a few kilobytes) could
also be stored in Datastore.

Please follow the [quick start guide](https://cloud.google.com/datastore/docs/quickstart)
to set up a database in Datastore mode. You may choose whichever region you
like, however, it can only be specified one and cannot be undone. Google Cloud
Platform currently allows only one Datastore database per project, so you
would need to create a new project to change the database location.

### Cloud Storage

Cloud Storage is used to store large blobs that don't fit in Datastore.

A sample Terraform configuration file is found at deploy/terraform/gcp/blobstore.tf.
You'll need to change the bucket names as necessary as Cloud Storage bucket names are
global resources. Alternatively, you may choose to create buckets with the Cloud Console.
Refer to the [Cloud Storage Documentation](https://cloud.google.com/storage/docs/creating-buckets)
to learn more about creating buckets.

## Updating the Cloud Build builder image

Note: You need permissions to the GCP project to make changes.

We use Cloud Build to run build tests for the GitHub repository. It is occasionally
necessary to update the base image to upgrade to a new Go version, add a new build
dependency, etc.

The base image is built using the [Dockerfile](../Dockerfile). It has two external dependencies.
- Go
- [protobuf](https://github.com/protocolbuffers/protobuf/)

Change the tag in the first line of to use a newer version of Go.

For example, to upgrade Go 1.17 to 1.18, change the line

```Dockerfile
FROM golang:1.17 AS base
```

to

```Dockerfile
FROM golang:1.18 AS base
```

To upgrade to a newer protobuf compiler, change the line with `PROTOC_VERSION`.

After changing the files, run `scripts/build_open_saves_builder_base.sh` to update the builder image.
The command takes a tag as an argument.
Change the tag when upgrading the toolkits or making significant changes to the builder.

For example, `1.17` would be the first builder with Go 1.17.
When upgrading Go to 1.18, change the tag to `1.18`.
If you're upgrading the protobuf compiler or making other changes, you can add a minor number such as `1.18.1`.

Change `cloudbuild.yaml` when you change the builder tag.
The builder is specified as

```yaml
  _BUILDER: "us-docker.pkg.dev/${PROJECT_ID}/open-saves-builder/open-saves-builder:1.17"
```

You can test the new configuration by running

```bash
gcloud builds submit . --project=triton-for-games-dev
```

## Updating public images

Cloud Build detects changes to the main branch and kicks off the build and deployment
of the `open-saves-server:testing` and `open-saves-collector:testing` images to Google
Container Registry. This is done by a Cloud Build Trigger with the Github App, and the
file that configures this is `cloudbuild_update_images.yaml`, using the substitution
variable `TAG_NAME = testing`. A similar trigger tags images with `:latest` when new
releases are detected, currently any new tags pushed.

These are not to be confused with `cloudbuild.yaml` which configures continuous
integration when pull requests are opened.

## IDE Support

Open Saves is a standard Go project so any IDE that understands that should
work. We use [Go Modules](https://github.com/golang/go/wiki/Modules) which is a
relatively new feature in Go so make sure the IDE you are using was built around
Summer 2019. The latest version of
[Visual Studio Code](https://code.visualstudio.com/download) supports it.

## Contributing to the project

Check out [How to Contribute](contributing.md) before contributing to the project.
