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

BIN_DIR = build
BIN_SUFFIX =
ifeq ($(OS),Windows_NT)
	BIN_SUFFIX = .exe
endif

SERVER_BIN = ${BIN_DIR}/server${BIN_SUFFIX}

ifeq ($(OS),Windows_NT)
	SERVER_BIN = ${BIN_DIR}/server.exe
endif

.PHONY: all clean test server protos FORCE

all: server

server: ${SERVER_BIN}

${SERVER_BIN}: protos FORCE
	go build -o $@ cmd/server/main.go

clean:
	rm -rf ${BIN_DIR}
	rm -f api/triton.pb.go

test:
	go test

protos: api/triton.pb.go

api/triton.pb.go: api/triton.proto
	bazel build ///api:triton_go_proto
	cp bazel-bin/api/triton_go_proto_/github.com/googleforgames/triton/triton.pb.go $@

FORCE:
