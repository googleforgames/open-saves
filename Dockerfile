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
# This is based on Debian and sets the GOPATH to /go.
# https://hub.docker.com/_/golang
FROM golang:1.17 AS builder

WORKDIR /src

# Copy local code to the container image.
COPY . ./open-saves

WORKDIR /src/open-saves

ENV GO111MODULE=on \
  CGO_ENABLED=0 \
  GOOS=linux \
  GOARCH=amd64

# Build the binary.
RUN go build -o build/server cmd/server/main.go 
RUN go build -o build/collector cmd/collector/main.go

# Build the garbage collector image.
#
# You can build the collector image by running
# docker build -t <tag> --target collector .

FROM alpine:3 AS collector
RUN apk add --no-cache ca-certificates
# Copy the binary to the production image from the builder stage.
COPY --from=builder /src/open-saves/build/collector /collector

CMD ["/collector"]

# Build the server image.
# This needs to be the last stage to be the default target.
#
# Use the official Alpine image for a lean production container.
# https://hub.docker.com/_/alpine
# https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds

FROM alpine:3 AS server
RUN apk add --no-cache ca-certificates

# Copy the binary to the production image from the builder stage.
COPY --from=builder /src/open-saves/build/server /server

# Run the web service on container startup.
CMD ["/server"]
