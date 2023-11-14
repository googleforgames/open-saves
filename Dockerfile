# Copyright 2021 Google LLC
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

# Use the official Golang image to create a build artifact.
# This base builder image will also used by Cloud Build.
# This is based on Debian and sets the GOPATH to /go.
# https://hub.docker.com/_/golang
FROM golang:1.20 AS builder

ENV PROTOC_VERSION=21.3
ENV GO111MODULE=on
ENV DEBIAN_FRONTEND="noninteractive"

RUN apt-get -q update && \
    apt-get install -qy unzip && \
    apt-get clean

RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip && \
    unzip -d protoc protoc-${PROTOC_VERSION}-linux-x86_64.zip && \
    cp protoc/bin/protoc /usr/local/bin && \
    cp -rf protoc/include/google /usr/local/bin && \
    rm -rf protoc

# The second step builds all binaries.
FROM builder AS amd64

WORKDIR /src

# Copy local code to the container image.
COPY . ./open-saves

WORKDIR /src/open-saves

ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64

# Build the binaries.
RUN make install-tools
RUN make

# Build the garbage collector image.
#
# You can build the collector image by running
# docker build -t <tag> --target collector .

FROM alpine:3 AS collector
RUN apk add --no-cache ca-certificates
RUN apk update && apk upgrade
# Copy the binary to the production image from the builder stage.
COPY --from=amd64 /src/open-saves/build/collector /collector

RUN GRPC_HEALTH_PROBE_VERSION=v0.4.22 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

CMD ["/collector"]

# Build the server image.
# This needs to be the last stage to be the default target.
#
# Use the official Alpine image for a lean production container.
# https://hub.docker.com/_/alpine
# https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds

FROM alpine:3 AS server
RUN apk add --no-cache ca-certificates
RUN apk update && apk upgrade

# Copy the binary to the production image from the builder stage.
COPY --from=amd64 /src/open-saves/build/server /server
COPY --from=amd64 /src/open-saves/configs /configs

RUN GRPC_HEALTH_PROBE_VERSION=v0.4.22 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

# Run the web service on container startup.
CMD ["/server"]
