# Development Guide

Triton is a service that platform agnostic cloud storage & save solution that
runs within [Kubernetes](https://kubernetes.io).

## Install Prerequisites

To build Triton you'll need the following applications installed.

 * [Git](https://git-scm.com/downloads)
 * [Go](https://golang.org/doc/install)
 * [Docker](https://docs.docker.com/install/) including the
   [post-install steps](https://docs.docker.com/install/linux/linux-postinstall/).
 * Bazel [https://docs.bazel.build/versions/master/install.html]

Optional Software

 * [Google Cloud Platform](cloud.google.com)
 * [VirtualBox](https://www.virtualbox.org/wiki/Downloads) or 
   Hyperkit[https://minikube.sigs.k8s.io/docs/reference/drivers/hyperkit/] for
   [Minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/).

*It's recommended that you install Go using their instructions because package
managers tend to lag behind the latest Go releases.*

## Get the Code

```bash
# Create a directory for the project.
mkdir -p $HOME/workspace
cd $HOME/workspace
# Download the source code.
git clone https://github.com/googleforgames/triton.git
cd triton 
```

*Typically for contributing you'll want to
[create a fork](https://help.github.com/en/articles/fork-a-repo) and use that
but for purpose of this guide we'll be using the upstream/master.*

## Building

TODO

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
