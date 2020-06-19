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
	"fmt"

	"github.com/golang/protobuf/ptypes/empty"
	tritonpb "github.com/googleforgames/triton/api"
	"github.com/googleforgames/triton/internal/app/blob"
	log "github.com/sirupsen/logrus"
)

type tritonServer struct {
	cloud     string
	blobStore blob.BlobStore
}

// newTritonServer creates a new instance of the triton server.
func newTritonServer(cloud string, bucket string) (tritonpb.TritonServer, error) {
	switch cloud {
	case "gcp":
		log.Infoln("Instantiating Triton server on GCP")
		gcs, err := blob.NewBlobGCP(bucket)
		if err != nil {
			return nil, err
		}
		triton := &tritonServer{
			cloud:     cloud,
			blobStore: gcs,
		}
		return triton, nil
	default:
		return nil, fmt.Errorf("cloud provider(%q) is not yet supported", cloud)
	}
}

func (s *tritonServer) CreateStore(ctx context.Context, req *tritonpb.CreateStoreRequest) (*tritonpb.Store, error) {
	log.Infof("created store: %+v", req.Store)
	return req.Store, nil
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

func (s *tritonServer) ListStores(ctx context.Context, req *tritonpb.ListStoresRequest) (*tritonpb.ListStoresResponse, error) {
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

func (s *tritonServer) QueryRecords(ctx context.Context, req *tritonpb.QueryRecordsRequest) (*tritonpb.QueryRecordsResponse, error) {
	return nil, nil
}
