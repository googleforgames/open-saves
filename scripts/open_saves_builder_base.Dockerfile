# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM debian:testing

ENV GO_VERSION=1.17.1
ENV GOPATH=/go
ENV SOURCE_DIR=/app
ENV BUILD_DIR=/build
ENV DEBIAN_FRONTEND="noninteractive"

RUN apt-get -q update && \
    apt-get -qy upgrade && \
    apt-get install -qy protobuf-compiler curl git make \
    build-essential redis-server && \
    apt-get clean

RUN curl -O https://dl.google.com/go/go${GO_VERSION}.linux-amd64.tar.gz && \
    tar -C /usr/local -xf go${GO_VERSION}.linux-amd64.tar.gz && \
    rm -f go${GO_VERSION}.linux-amd64.tar.gz && \
    mkdir ${GOPATH}

ENV PATH /usr/local/go/bin:/go/bin:$PATH

RUN go get -u \
    golang.org/x/lint/golint \
    google.golang.org/protobuf/cmd/protoc-gen-go \
    google.golang.org/grpc/cmd/protoc-gen-go-grpc \
    github.com/golang/mock/mockgen
