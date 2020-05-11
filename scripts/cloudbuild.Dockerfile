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

FROM gcr.io/triton-for-games-dev/triton-builder-base:latest

ENV GO_VERSION=1.14.2
ENV GOPATH=/go
ENV SOURCE_DIR=/app
ENV BUILD_DIR=/build
ENV DEBIAN_FRONTEND="noninteractive"

COPY . ${SOURCE_DIR}

WORKDIR ${SOURCE_DIR}
# RUN go get ./...

RUN echo "export PATH=/usr/local/go/bin:/go/bin/:\$PATH" >> /root/.bashrc
RUN echo "export CXX=clang++-10" >> /root/.bashrc
RUN echo "export GOPATH=/go" >> /root/.bashrc
RUN mkdir ${BUILD_DIR}
WORKDIR ${BUILD_DIR}

ENV CXX=clang++-10
RUN cmake -S ${SOURCE_DIR}

