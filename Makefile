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
API_DIR = api
PROTOC = protoc

SERVER_BIN = ${BIN_DIR}/server
COLLECTOR_BIN = ${BIN_DIR}/collector
OPENSAVES_GO_PROTOS = ${API_DIR}/open_saves.pb.go ${API_DIR}/open_saves_grpc.pb.go
ALL_TARGETS = ${SERVER_BIN} ${COLLECTOR_BIN} ${OPENSAVES_GO_PROTOS}

.PHONY: all clean test server protos swagger FORCE

all: server collector

server: ${SERVER_BIN}

collector: ${COLLECTOR_BIN}

${SERVER_BIN}: cmd/server/main.go protos FORCE
	go build -o $@ $<

${COLLECTOR_BIN}: cmd/collector/main.go protos FORCE
	go build -o $@ $<

clean:
	rm -f ${ALL_TARGETS}

test: server
	go test -race -v ./...

protos: ${OPENSAVES_GO_PROTOS}

${OPENSAVES_GO_PROTOS}: ${API_DIR}/open_saves.proto
	$(PROTOC) -I. \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
  		$<

FORCE:
