// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/uuid"
	tritonpb "github.com/googleforgames/triton/api"
)

type tritonServer struct{}

// newTritonServer creates a new instance of the triton server.
func newTritonServer() tritonpb.TritonServer {
	return new(tritonServer)
}

func (s *tritonServer) CreateStore(ctx context.Context, req *tritonpb.CreateStoreRequest) (*tritonpb.Store, error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	store := &tritonpb.Store{
		Id:   int64(id.ID()),
		Name: req.Name,
	}
	return store, nil
}

func (s *tritonServer) CreateRecord(ctx context.Context, req *tritonpb.CreateRecordRequest) (*tritonpb.Record, error) {
	return nil, nil
}

func (s *tritonServer) DeleteRecord(ctx context.Context, req *tritonpb.DeleteRecordRequest) (*empty.Empty, error) {
	return nil, nil
}

func (s *tritonServer) GetStore(ctx context.Context, req *tritonpb.GetStoreRequest) (*tritonpb.Store, error) {
	return nil, nil
}

func (s *tritonServer) ListStore(ctx context.Context, req *tritonpb.ListStoresRequest) (*tritonpb.ListStoresResponse, error) {
	return nil, nil
}

func (s *tritonServer) DeleteStore(ctx context.Context, req *tritonpb.DeleteStoreRequest) (*empty.Empty, error) {
	return nil, nil
}

func (s *tritonServer) GetRecord(ctx context.Context, req *tritonpb.GetRecordRequest) (*tritonpb.Record, error) {
	return nil, nil
}

func (s *tritonServer) UpdateRecord(ctx context.Context, req *tritonpb.UpdateRecordRequest) (*tritonpb.Record, error) {
	return nil, nil
}
