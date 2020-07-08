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
TRITON_GO_PROTOS = ${API_DIR}/triton.pb.go ${API_DIR}/triton.pb.gw.go
TRITON_SWAGGER_JSON = ${API_DIR}/triton.swagger.json
ALL_TARGETS = ${SERVER_BIN} ${TRITON_GO_PROTOS} ${TRITON_SWAGGER_JSON}

.PHONY: all clean test server protos swagger mock FORCE

all: server

server: ${SERVER_BIN}

${SERVER_BIN}: cmd/server/main.go protos FORCE
	go build -o $@ $<

clean:
	rm -f ${ALL_TARGETS}

test:
	go test -race -v ./...

protos: ${TRITON_GO_PROTOS}

swagger: ${TRITON_SWAGGER_JSON}

mock: internal/pkg/metadb/mock/mock_metadb.go

internal/pkg/metadb/mock/mock_metadb.go: internal/pkg/metadb/metadb.go
	mockgen -source "$<" -destination "$@"

${API_DIR}/triton.pb.go: ${API_DIR}/triton.proto
	$(PROTOC) -I. \
  		-I${GOPATH}/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
 		--go_out=plugins=grpc:. \
		--go_opt=paths=source_relative \
  		$<

${API_DIR}/triton.pb.gw.go: ${API_DIR}/triton.proto
	$(PROTOC) -I. \
  		-I${GOPATH}/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
		--grpc-gateway_out=logtostderr=true,paths=source_relative:. \
  		$<

${API_DIR}/triton.swagger.json: ${API_DIR}/triton.proto
	$(PROTOC) -I. \
  		-I${GOPATH}/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
  		--swagger_out=logtostderr=true:. \
		$<

FORCE: