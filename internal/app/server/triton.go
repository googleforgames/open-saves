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
	"github.com/googleforgames/triton/internal/pkg/blob"
	"github.com/googleforgames/triton/internal/pkg/cache"
	"github.com/googleforgames/triton/internal/pkg/metadb"
	"github.com/googleforgames/triton/internal/pkg/metadb/datastore"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/status"
)

type tritonServer struct {
	cloud      string
	blobStore  blob.BlobStore
	metaDB     *metadb.MetaDB
	cacheStore cache.Cache
}

// newTritonServer creates a new instance of the triton server.
func newTritonServer(ctx context.Context, cloud, project, bucket, cacheAddr string) (tritonpb.TritonServer, error) {
	switch cloud {
	case "gcp":
		log.Infoln("Instantiating Triton server on GCP")
		gcs, err := blob.NewBlobGCP(bucket)
		if err != nil {
			return nil, err
		}
		datastore, err := datastore.NewDriver(ctx, project)
		if err != nil {
			return nil, err
		}
		metadb := metadb.NewMetaDB(datastore)
		if err := metadb.Connect(ctx); err != nil {
			log.Fatalf("Failed to connect to the lta server: %v", err)
			return nil, err
		}
		redis := cache.NewRedis(cacheAddr)
		triton := &tritonServer{
			cloud:      cloud,
			blobStore:  gcs,
			metaDB:     metadb,
			cacheStore: redis,
		}
		return triton, nil
	default:
		return nil, fmt.Errorf("cloud provider(%q) is not yet supported", cloud)
	}
}

func (s *tritonServer) CreateStore(ctx context.Context, req *tritonpb.CreateStoreRequest) (*tritonpb.Store, error) {
	store := metadb.Store{
		Key:     req.Store.Key,
		Name:    req.Store.Name,
		Tags:    req.Store.Tags,
		OwnerID: req.Store.OwnerId,
	}
	if err := s.metaDB.CreateStore(ctx, &store); err != nil {
		log.Errorf("CreateStore failed for store (%s): %v", store.Key, err)
		return nil, status.Convert(err).Err()
	}
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
