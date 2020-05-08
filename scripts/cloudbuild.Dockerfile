FROM ubuntu:20.04

ENV GO_VERSION=1.14.2
ENV GOPATH=/go
ENV SOURCE_DIR=/app
ENV BUILD_DIR=/build
ENV DEBIAN_FRONTEND="noninteractive"

RUN apt-get -q update && \
    apt-get -qy upgrade && \
    apt-get install -qy cmake protobuf-compiler clang-10 \
        curl git build-essential autoconf libtool pkg-config \
        libgrpc++-dev

RUN curl -O https://dl.google.com/go/go${GO_VERSION}.linux-amd64.tar.gz && \
    tar -C /usr/local -xf go${GO_VERSION}.linux-amd64.tar.gz && \
    rm -f go${GO_VERSION}.linux-amd64.tar.gz && \
    mkdir ${GOPATH}

ENV PATH /usr/local/go/bin:/go/bin:$PATH

RUN go get -u \
    golang.org/x/lint/golint \
    github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway \
    github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger \
    github.com/golang/protobuf/protoc-gen-go
